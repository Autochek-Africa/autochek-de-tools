import unittest
from unittest.mock import patch, MagicMock
import os
import pandas as pd
from autochektools.common import FileUtil

class TestFileUtil(unittest.TestCase):

    @patch("os.path.join") 
    @patch("pandas.DataFrame.to_csv")
    def test_save_csv_to_file(self, mock_to_csv, mock_os_path_join):
    
        mock_df = MagicMock(spec=pd.DataFrame)
        tempdirectory = "/fake/tempdir"
        filename = "test_file.csv"
        
    
        expected_file_path = "/fake/tempdir/test_file.csv"
        
    
        mock_os_path_join.return_value = expected_file_path
        mock_to_csv.return_value = None

        result = FileUtil.save_csv_to_file(mock_df, tempdirectory, filename)

        mock_os_path_join.assert_called_once_with(tempdirectory, filename)
        
        mock_df.to_csv.assert_called_once_with(expected_file_path, index=False)
        
        self.assertEqual(result, expected_file_path)

    @patch("os.path.join")
    @patch("pandas.DataFrame.to_parquet")
    def test_save_parquet_to_file(self, mock_to_parquet, mock_os_path_join):

        mock_df = MagicMock(spec=pd.DataFrame)
        tempdirectory = "/fake/tempdir"
        filename = "test_file.parquet"
        
        expected_file_path = "/fake/tempdir/test_file.parquet"
        
        mock_os_path_join.return_value = expected_file_path
        mock_to_parquet.return_value = None

        result = FileUtil.save_parquet_to_file(mock_df, tempdirectory, filename)

        mock_os_path_join.assert_called_once_with(tempdirectory, filename)
        
        mock_df.to_parquet.assert_called_once_with(expected_file_path, index=False)
        
        self.assertEqual(result, expected_file_path)

if __name__ == "__main__":
    unittest.main()
