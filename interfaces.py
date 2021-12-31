import abc
import pandas as pd

class FilterObject(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def aplly_filters_and_save_df_to_csv(
        self, 
        df: pd.DataFrame, 
        df_dic: pd.DataFrame,
        dest_folder: str, 
        souce_file_name: str, 
        file_year: str):

        raise NotImplemented