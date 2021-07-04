def diff_columns(raw_df, processed_df):
    
    '''
    This check compares number of columns between processed and raw datasets
    '''
    
    raw_df_columns = len(raw_df.columns)
    processed_df_columns = len(processed_df.columns)
    
    if(len(raw_df.columns) == len(processed_df.columns)):
        print(f"Processed and raw datasets contain the same number of columns: {raw_df_columns}")
    else:
        print(f"There's a difference in number of columns between processed and raw datasets.\n This is due to columns having more than 50% of population as null.\n Processed dataset has {processed_df_columns} columns.\n Raw dataset has {raw_df_columns} columns.")

def diff_records(raw_df, processed_df):
    
    '''
    This check compares number of records between processed and raw datasets
    '''
    
    processed_df_count = processed_df.count()
    raw_df_count = raw_df.count()
    
    if (raw_df_count == processed_df_count):
        print('Checking between processed and raw data, there were no duplicates and empty rows in raw data')
    else:
        missing_records = raw_df_count - processed_df_count
        print(f'Checking between processed and raw data, there were {missing_records} duplicates and empty rows in raw data')
        