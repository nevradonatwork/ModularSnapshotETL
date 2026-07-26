from unittest.mock import patch

from src import visitor_log


class TestClientMetadata:
    def test_extracts_ip_and_user_agent_from_headers(self):
        headers = {"X-Forwarded-For": "203.0.113.5, 10.0.0.1", "User-Agent": "TestBrowser/1.0"}
        with patch("streamlit.context") as mock_context:
            mock_context.headers = headers
            meta = visitor_log._get_client_metadata()
        assert meta["ip"] == "203.0.113.5"
        assert meta["user_agent"] == "TestBrowser/1.0"

    def test_falls_back_to_real_ip_header(self):
        headers = {"X-Real-Ip": "203.0.113.9", "User-Agent": "TestBrowser/1.0"}
        with patch("streamlit.context") as mock_context:
            mock_context.headers = headers
            meta = visitor_log._get_client_metadata()
        assert meta["ip"] == "203.0.113.9"

    def test_swallows_errors_to_none(self):
        # st.context.headers raises AttributeError when context is None
        with patch("streamlit.context", None):
            meta = visitor_log._get_client_metadata()
        assert meta == {"ip": None, "user_agent": None}


class TestGeolocateIp:
    def test_returns_empty_for_none(self):
        assert visitor_log._geolocate_ip(None) == ("", "")

    def test_returns_empty_for_private_ip(self):
        assert visitor_log._geolocate_ip("10.0.0.1") == ("", "")
        assert visitor_log._geolocate_ip("127.0.0.1") == ("", "")

    def test_returns_empty_for_invalid_ip(self):
        assert visitor_log._geolocate_ip("not-an-ip") == ("", "")

    def test_parses_successful_response(self):
        import json

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return json.dumps({"status": "success", "city": "Istanbul", "country": "Turkey"}).encode()

        with patch("urllib.request.urlopen", return_value=FakeResponse()):
            city, country = visitor_log._geolocate_ip("8.8.8.8")
        assert city == "Istanbul"
        assert country == "Turkey"

    def test_swallows_network_errors(self):
        with patch("urllib.request.urlopen", side_effect=OSError("timeout")):
            assert visitor_log._geolocate_ip("8.8.8.8") == ("", "")


class TestVisitTracking:
    def test_start_visit_inserts_row(self, db_conn):
        with patch.object(visitor_log, "_get_client_metadata", return_value={"ip": "8.8.8.8", "user_agent": "UA"}), \
             patch.object(visitor_log, "_geolocate_ip", return_value=("Istanbul", "Turkey")):
            session_uuid = visitor_log.start_visit(db_conn)

        row = db_conn.execute(
            "SELECT session_uuid, ip_address, city, country, ran_pipeline, page_views "
            "FROM visitor_log WHERE session_uuid = ?",
            (session_uuid,),
        ).fetchone()
        assert row == (session_uuid, "8.8.8.8", "Istanbul", "Turkey", 0, 1)

    def test_touch_visit_increments_page_views(self, db_conn):
        with patch.object(visitor_log, "_get_client_metadata", return_value={"ip": None, "user_agent": None}), \
             patch.object(visitor_log, "_geolocate_ip", return_value=("", "")):
            session_uuid = visitor_log.start_visit(db_conn)

        visitor_log.touch_visit(db_conn, session_uuid)
        visitor_log.touch_visit(db_conn, session_uuid)

        row = db_conn.execute(
            "SELECT page_views FROM visitor_log WHERE session_uuid = ?", (session_uuid,)
        ).fetchone()
        assert row[0] == 3

    def test_mark_ran_pipeline_sets_flag(self, db_conn):
        with patch.object(visitor_log, "_get_client_metadata", return_value={"ip": None, "user_agent": None}), \
             patch.object(visitor_log, "_geolocate_ip", return_value=("", "")):
            session_uuid = visitor_log.start_visit(db_conn)

        visitor_log.mark_ran_pipeline(db_conn, session_uuid)

        row = db_conn.execute(
            "SELECT ran_pipeline FROM visitor_log WHERE session_uuid = ?", (session_uuid,)
        ).fetchone()
        assert row[0] == 1
