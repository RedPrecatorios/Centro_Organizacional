import unittest
from unittest.mock import patch

from messages_viewer.api_calculo_monitor import _job_summary


class TestForwardTag(unittest.TestCase):
    def test_caso_recebido_mostra_ip_da_outra_api(self) -> None:
        with patch(
            "messages_viewer.api_calculo_monitor._peer_ip",
            return_value="192.81.217.95",
        ):
            summary = _job_summary(
                {
                    "job_id": "a" * 32,
                    "status": "queued",
                    "source_ip": "165.227.67.80",
                    "offload_hops": 1,
                    "priority_rank": 2,
                    "payload": {"record": {"Requerente": "Ana"}},
                }
            )
        self.assertEqual(summary["forwarded_from"], "192.81.217.95")
        self.assertEqual(summary["source_ip"], "165.227.67.80")

    def test_caso_local_nao_ganha_tag_da_outra_api(self) -> None:
        with patch(
            "messages_viewer.api_calculo_monitor._peer_ip",
            return_value="192.81.217.95",
        ):
            summary = _job_summary(
                {
                    "job_id": "b" * 32,
                    "status": "queued",
                    "source_ip": "68.183.23.91",
                    "offload_hops": 0,
                    "payload": {"record": {"Requerente": "Bia"}},
                }
            )
        self.assertNotIn("forwarded_from", summary)


if __name__ == "__main__":
    unittest.main()
