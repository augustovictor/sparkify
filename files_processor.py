class FilesProcessor:
    def __init__(self, all_files, conn, cur, func, num_files):
        self.all_files = all_files
        self.conn = conn
        self.cur = cur
        self.func = func
        self.num_files = num_files

    def process_files(self):
        """
        Processes given files
        """

        for i, datafile in enumerate(self.all_files, 1):
            self.func(self.cur, datafile)
            self.conn.commit()
            self._print_processing_progress(i, self.num_files)

    def _print_processing_progress(self, i, num_files):
        """
        Encapsulates how processing progress is printed
        """

        print('{}/{} files processed.'.format(i, num_files))