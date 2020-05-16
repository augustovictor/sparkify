import os
import glob

class FilePathsAppender:
    def __init__(self, cur, conn, filepath, filepath_pattern):
        self.cur = cur
        self.conn = conn
        self.filepath = filepath
        self.filepath_pattern = filepath_pattern

    def get_files_in(self):
        """
        Returns all file paths that match a given pattern
        """
        all_files = []
        for root, dirs, files in os.walk(self.filepath):
            files = glob.glob(os.path.join(root, self.filepath_pattern))
            for f in files:
                all_files.append(os.path.abspath(f))
        return all_files

    def print_files_count_for(self, filepath, num_files):
        """
        Encapsulates how found filepaths are formatted and printed
        """

        print('{} files found in {}'.format(num_files, filepath))