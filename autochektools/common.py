import os


class FileUtil:
    @staticmethod
    def save_csv_to_file(df, tempdirectory, filename):
        file_path = os.path.join(tempdirectory, filename)
        df.to_csv(file_path, index=False)

        return file_path

    @staticmethod
    def save_parquet_to_file(df, tempdirectory, filename):
        file_path = os.path.join(tempdirectory, filename)
        df.to_parquet(file_path, index=False)

        return file_path
