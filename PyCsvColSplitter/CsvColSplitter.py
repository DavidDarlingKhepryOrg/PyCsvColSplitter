import csv
import io
import logging
import os
import sqlite3
import sys
import time

src_file_fldr = '/home/data/voters/nc'
src_file_name = 'ncvoter92.csv'

tmp_source_file_fldr = 'temp/source'
tmp_sorted_file_fldr = 'temp/sorted'
tmp_grouped_file_fldr = 'temp/grouped'

logger = logging.getLogger('')

class ColSplitter:
    
    def __init__(self):
        pass
            
    def expand_path(self,
                    file_name: str=None,
                    file_fldr: str=None):
        
        file_path = None
        
        join_file_fldr_and_name = False
        
        if file_fldr is not None and file_fldr.startswith('~'):
            file_path = os.path.expanduser(file_fldr)
            
        if file_name is not None:

            if file_name.startswith('~'):
                file_path = os.path.expanduser(file_name)
            
            if file_name.startswith('/'):
                file_path = os.path.abspath(file_name)

            join_file_fldr_and_name = True and file_fldr is not None
            
        if join_file_fldr_and_name:
            file_path = os.path.join(file_fldr, file_name)
        elif file_path is None:
            if file_fldr is not None:
                file_path = file_fldr
            else:
                file_path = file_name

        file_path = os.path.abspath(file_path)
        
        return file_path


    def path_exists(self,
                    file_path: str,
                    log_messages: bool=True,
                    non_exist_is_error: bool=False):
        
        it_exists = os.path.exists(file_path)
        
        if log_messages:
            logger.info('file_path exists YES? "%s"', file_path)
            if it_exists:
                logger.info('file_path exists YES! "%s"', file_path)
            else:
                err_str = 'file_path exists NOT! "%s"' % file_path
                if non_exist_is_error:
                    logger.error(err_str)
                else:
                    logger.info(err_str)
        
        return it_exists
    

    def split_cols_into_files(self,
                              src_file_name: str,
                              src_file_fldr: str,
                              tmp_file_fldr: str,
                              flush_size: int=1000,
                              show_progress_msgs: bool=True):
        
        src_file_path = self.expand_path(file_name=src_file_name,
                                         file_fldr=src_file_fldr)

        if self.path_exists(file_path=src_file_path):
            rows = 0
            with io.open(src_file_path, 'r', newline='') as csv_file:
                
                csv_dict_reader = csv.DictReader(csv_file)
                
                field_names = csv_dict_reader.fieldnames
                
                field_name_files = {}
                field_name_writers = {}
                
                tmp_file_path = self.expand_path(file_fldr=tmp_file_fldr)
                if not self.path_exists(file_path=tmp_file_path):
                    os.makedirs(tmp_file_path, exist_ok=True)
                    logger.info('tmp_file_path: "%s"', tmp_file_path)
                
                for field_name in field_names:
                    tmp_file_path = self.expand_path(field_name + '.csv', tmp_file_fldr)
                    field_name_files[field_name] = io.open(tmp_file_path, 'w', newline='')
                    field_name_writers[field_name] = csv.writer(field_name_files[field_name], delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    field_name_writers[field_name].writerow(['row', field_name])
                    

                # beginning time hack
                bgn_time = time.time()
                
                prev_rows = 0
                
                for row in csv_dict_reader:
                    rows += 1
                    for field_name in field_names:
                        field_name_writers[field_name].writerow([rows, row[field_name]])
                        if rows % flush_size == 0:
                            field_name_files[field_name].flush()
    
                    if show_progress_msgs:
                        if rows % flush_size == 0 and rows - prev_rows >= flush_size:
                            # ending time hack
                            end_time = time.time()
                            # compute records/second
                            seconds = end_time - bgn_time
                            if seconds > 0:
                                rcds_per_second = rows / seconds
                                flush_size = int(rcds_per_second)
                                prev_rows = rows
                            else:
                                rcds_per_second = 0
                            # output progress message
                            message = "Processed: {:,} rows in {:,.0f} seconds @ {:,.0f} records/second".format(rows, seconds, rcds_per_second)
                            logger.info(message)
                                        
                for field_name in field_names:
                    field_name_files[field_name].close()

                # ending time hack
                end_time = time.time()
                # compute records/second
                seconds = end_time - bgn_time
                if seconds > 0:
                    rcds_per_second = rows / seconds
                else:
                    rcds_per_second = 0
                # output progress message
                message = "Processed: {:,} rows in {:,.0f} seconds @ {:,.0f} records/second".format(rows, seconds, rcds_per_second)
                logger.info(message)
                
        return
    
    
    def sort_rows_into_file(self,
                            src_file_path: str,
                            tmp_sorted_fldr: str,
                            tmp_grouped_fldr: str,
                            flush_size: int=1000,
                            show_progress_msgs: bool=True):
                    
        
        if self.path_exists(src_file_path):
            conn = sqlite3.connect(':memory:')
            conn.execute('create table sorted(id, value)')
#             conn.execute('create index ndx_sorted on sorted (col_name, value)')

            base_name = os.path.basename(src_file_path)
            col_name = os.path.splitext(base_name)[0]

            # beginning time hack
            bgn_time = time.time()
            
            with io.open(src_file_path, 'r', newline='') as csv_file:
                csv_file_reader = csv.reader(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                values = []
                rows = 0
                for row in csv_file_reader:
                    rows += 1
                    if rows > 1:
                        # row.append(col_name)
                        values.append(row)
                        if rows % flush_size == 0:
                            conn.executemany('insert into sorted(id, value) values(?,?);', values)
                            conn.commit()
    
                            if show_progress_msgs:
                                # ending time hack
                                end_time = time.time()
                                # compute records/second
                                seconds = end_time - bgn_time
                                if seconds > 0:
                                    rcds_per_second = rows / seconds
                                    flush_size = int(rcds_per_second)
                                else:
                                    rcds_per_second = 0
                                # output progress message
                                message = "Processed: {:,} rows in {:,.0f} seconds @ {:,.0f} records/second".format(rows, seconds, rcds_per_second)
                                logger.info(message)
                if rows > 0:
                    conn.commit()

            # ending time hack
            end_time = time.time()
            # compute records/second
            seconds = end_time - bgn_time
            if seconds > 0:
                rcds_per_second = rows / seconds
            else:
                rcds_per_second = 0
            # output progress message
            message = "Processed: {:,} rows in {:,.0f} seconds @ {:,.0f} records/second".format(rows, seconds, rcds_per_second)
            logger.info(message)

            # output the "sorted" CSV files

            tmp_sorted_path = self.expand_path(file_name=base_name, file_fldr=tmp_sorted_fldr)
            
            if not self.path_exists(file_path=os.path.dirname(tmp_sorted_path)):
                os.makedirs(os.path.dirname(tmp_sorted_path), exist_ok=True)
                logger.info('tmp_sorted_path: "%s"', tmp_sorted_path)
            
            with io.open(tmp_sorted_path, 'w', newline='') as out_file:
                csv_file_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csv_file_writer.writerow(['id', 'value'])            
                cursor_sorted = conn.execute('select id, value from sorted order by value')
                row_list = cursor_sorted.fetchmany(flush_size)
                while len(row_list) > 0:
                    for row in row_list:
                        csv_file_writer.writerow(row)           
                    row_list = cursor_sorted.fetchmany(flush_size)
                cursor_sorted.close()

            # output the "grouped" CSV files
            
            tmp_grouped_path = self.expand_path(file_name=base_name, file_fldr=tmp_grouped_fldr)
            
            if not self.path_exists(file_path=os.path.dirname(tmp_grouped_path)):
                os.makedirs(os.path.dirname(tmp_grouped_path), exist_ok=True)
                logger.info('tmp_grouped_path: "%s"', tmp_grouped_path)
            
            with io.open(tmp_grouped_path, 'w', newline='') as out_file:
                csv_file_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csv_file_writer.writerow(['value', 'count'])            
                cursor_grouped = conn.execute('select value, count(*) from sorted group by value order by value')
                row_list = cursor_grouped.fetchmany(flush_size)
                while len(row_list) > 0:
                    for row in row_list:
                        csv_file_writer.writerow(row)           
                    row_list = cursor_grouped.fetchmany(flush_size)
                cursor_grouped.close()
                
            conn.execute('delete from sorted;')
                        
            conn.close()
                        
        
        return


# ==============================
# implement minimum log filter
# for restraining logging output
# ==============================
class MinLogLevelFilter(logging.Filter):
    '''
    Minimum Log Level Filter class
    '''
    def __init__(self, level):
        '''

        :param level:
        '''
        self.level = level

    def filter(self, record):
        '''

        :param record:
        '''
        return record.levelno >= self.level


# ==============================
# implement maximum log filter
# for restraining logging output
# ==============================
class MaxLogLevelFilter(logging.Filter):
    '''
    Maximum Log Level Filter class
    '''
    def __init__(self, level):
        '''

        :param level:
        '''
        self.level = level

    def filter(self, record):
        '''

        :param record:
        '''
        return record.levelno <= self.level


def get_logger(log_name: str='',
               max_stdout_level: int=logging.INFO,
               min_stdout_level: int=logging.INFO,
               min_stderr_level: int=logging.WARNING,
               default_msg_format: str='%(asctime)s\t%(levelname)s\t%(module)s\t%(funcName)s\t%(lineno)d\t%(message)s',
               default_date_format: str='%Y-%m-%d %H:%M:%S',
               all_log_file_name: str=None,
               err_log_file_name: str=None,
               log_file_mode: str='a'):

    err_str = None
    logger = logging

    allLoggerFH = None
    errLoggerFH = None

    # set the default logger's values
    logging.basicConfig(level=max_stdout_level,
                        format=default_msg_format,
                        datefmt=default_date_format)

    try:
        # instantiate the logger object
        logger = logging.getLogger(log_name)
        # remove log handlers
        logger.handlers = []
    except Exception as err:
        err_str = str(err)
        logging.error('Error instantiating a logger object named: %s', log_name)
        logging.error(err_str)

    if err_str is None:
        try:
            # remove log handlers
            logger.handlers = []
        except Exception as err:
            err_str = str(err)
            logging.error('Error clearing handlers for logger named: %s', log_name)
            logging.error(err_str)

    if err_str is None:
        try:
            # attach stdout to the logger
            # so that outputting to the log also
            # outputs to the stdout console
            logStdOut = logging.StreamHandler(sys.stdout)
            logStdOut.setFormatter(logging.Formatter(default_msg_format, default_date_format))
            logStdOut.addFilter(MaxLogLevelFilter(max_stdout_level))
            logStdOut.setLevel(logging.DEBUG)
            logger.addHandler(logStdOut)
        except Exception as err:
            err_str = str(err)
            logging.error('Error instantiating "stdout" handler for logger named: %s', log_name)
            logging.error(err_str)

    if err_str is None:
        try:
            # attach stderr to the logger
            # so that outputting to the log also
            # outputs to the stderr console
            logStdErr = logging.StreamHandler(sys.stderr)
            logStdErr.setFormatter(logging.Formatter(default_msg_format, default_date_format))
            logStdErr.addFilter(MinLogLevelFilter(min_stderr_level))
            logStdErr.setLevel(minStdErrLvl)
            logger.addHandler(logStdErr)
        except Exception as err:
            err_str = str(err)
            logging.error('Error instantiating "stderr" handler for logger named: %s', log_name)
            logging.error(err_str)

    if err_str is None and all_log_file_name is not None:
        try:
            # instantiate the "all" logging file handler
            allLoggerFH = logging.FileHandler(all_log_file_name)
            allLoggerFH.setLevel(max_stdout_level)
            allLoggerFH.setFormatter(logging.Formatter(default_msg_format, default_date_format))
            logger.addHandler(allLoggerFH)
        except Exception as err:
            err_str = str(err)
            logging.error('Error instantiating "all" log handler for logger named: %s', log_name)
            logging.error(err_str)

    if err_str is None and err_log_file_name is not None:
        try:
            # instantiate the "all" logging file handler
            errLoggerFH = logging.FileHandler(err_log_file_name)
            errLoggerFH.setLevel(min_stderr_level)
            errLoggerFH.setFormatter(logging.Formatter(default_msg_format, default_date_format))
            logger.addHandler(errLoggerFH)
        except Exception as err:
            err_str = str(err)
            logging.error('Error instantiating "err" log handler for logger named: %s', log_name)
            logging.error(err_str)

    # set the nominal log level
    try:
        logger.setLevel(min_stdout_level)
    except Exception as err:
        err_str = str(err)
        logging.error('Error setting nominal log level for logger named: %s:', log_name)
        logging.error(err_str)

    return logger, allLoggerFH, errLoggerFH, err_str


if __name__ == "__main__":

    # maximum logging level that
    # will output to the STDOUT stream
    MAX_STDOUT_LEVEL = logging.INFO
    MIN_STDERR_LEVEL = logging.WARNING
    
    maxStdOutLvl = MAX_STDOUT_LEVEL
    minStdErrLvl = MIN_STDERR_LEVEL
    
    logger, _allLoggerFH, _errLoggerFH, err_str = get_logger()
    
    colSplitter = ColSplitter()
    colSplitter.split_cols_into_files(src_file_name=src_file_name,
                                      src_file_fldr=src_file_fldr,
                                      tmp_file_fldr=tmp_source_file_fldr)
    
    for root, dirs, files in os.walk(tmp_source_file_fldr):
        for file in files:
            src_file_path = os.path.join(root, file)
            logger.info('Sorting file: "%s"', src_file_path)
            colSplitter.sort_rows_into_file(src_file_path=src_file_path,
                                            tmp_sorted_fldr=tmp_sorted_file_fldr,
                                            tmp_grouped_fldr=tmp_grouped_file_fldr)