import os
import glob

class FileProcessor:
    def __init__(self, cur, conn, filepath, func, filepath_pattern):
        self.cur = cur
        self.conn = conn
        self.filepath = filepath
        self.func = func
        self.filepath_pattern = filepath_pattern

    def get_files_in(self, filepath, filepath_pattern):
        """
        Returns all file paths that match a given pattern
        """
        all_files = []
        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root, filepath_pattern))
            for f in files:
                all_files.append(os.path.abspath(f))
        return all_files

    def print_files_count_for(self, filepath, num_files):
        """
        Encapsulates how found filepaths are formatted and printed
        """

        print('{} files found in {}'.format(num_files, filepath))

    def process_files(self, all_files, conn, cur, func, num_files):
        """
        Processes given files
        """

        for i, datafile in enumerate(all_files, 1):
            func(cur, datafile)
            conn.commit()
            self._print_processing_progress(i, num_files)

    def _print_processing_progress(self, i, num_files):
        """
        Encapsulates how processing progress is printed
        """

        print('{}/{} files processed.'.format(i, num_files))