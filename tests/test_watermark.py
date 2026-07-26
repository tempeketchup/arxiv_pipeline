import json
from unittest.mock import MagicMock, patch

from scripts import watermark


def test_get_last_watermark_returns_default_when_no_file_exists():
    with patch.object(watermark, '_get_file_client') as mock_get_client:
        mock_client = MagicMock()
        mock_client.download_file.side_effect = Exception('blob not found')
        mock_get_client.return_value = mock_client

        result = watermark.get_last_watermark('acct', 'container', default='2020-01-01T00:00:00Z')
        
        assert result == '2020-01-01T00:00:00Z'

def test_get_last_watermark_returns_stored_value():
    with patch.object(watermark, '_get_file_client') as mock_get_client:
        mock_client = MagicMock()
        mock_download = MagicMock()
        mock_download.readall.return_value = b'{"last_submitted_date": "2024-06-01T00:00:00Z"}'
        mock_client.download_file.return_value = mock_download 
        mock_get_client.return_value = mock_client

        result = watermark.get_last_watermark('acct', 'container')
        
        assert result == '2024-06-01T00:00:00Z'

def test_update_watermark_uploads_expected_payload():
    with patch.object(watermark, '_get_file_client') as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client  

        watermark.update_watermark('acct', 'container', '2024-07-01T00:00:00Z')

        mock_client.upload_data.assert_called_once()
        args, kwargs = mock_client.upload_data.call_args
        payload = json.loads(args[0])
        assert payload['last_submitted_date'] == '2024-07-01T00:00:00Z'
        assert kwargs.get('overwrite') is True