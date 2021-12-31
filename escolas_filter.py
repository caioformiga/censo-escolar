import interfaces
import param 
import censo_escolar_utils as utils

# pip install pandas
import pandas as pd

from logging import error

class FilterOfEscolas(interfaces.FilterObject):
        
    def aplly_filters_and_save_df_to_csv(self, df: pd.DataFrame, df_dic: pd.DataFrame, dest_folder, souce_file_name, file_year):
        
        # usa o dicionario e recupera a lista de colunas que devem ser salvas
        col_names = df_dic.Col_Name.tolist()

        filter_name = 'BA'        
        df_filter = self.filter_bahia_and_save_df_to_csv(df, df_dic, dest_folder, souce_file_name, file_year, filter_name, col_names)        
        df_filter_anexos = self.filter_anexo_and_save_df_to_csv(df_filter, df_dic, dest_folder, souce_file_name, file_year, filter_name, col_names)

        if (df_filter.empty == True):
            return error('Erro em filter_bahia_and_save_df_to_csv')          

        df = df_filter
        filter_name = filter_name + '_ESTADUAL'
        df_filter = self.filter_estadual_and_save_df_to_csv(df, df_dic, dest_folder, souce_file_name, file_year, filter_name, col_names)
        df_filter_anexos = self.filter_anexo_and_save_df_to_csv(df_filter, df_dic, dest_folder, souce_file_name, file_year, filter_name, col_names)

        if (df_filter.empty == True):
            return error('Erro em filter_estadual_and_save_df_to_csv')
        
        df = df_filter
        filter_name = filter_name + '_RURAL'
        df_filter = self.filter_rural_and_save_df_to_csv(df, df_dic, dest_folder, souce_file_name, file_year, filter_name, col_names)
        df_filter_anexos = self.filter_anexo_and_save_df_to_csv(df_filter, df_dic, dest_folder, souce_file_name, file_year, filter_name, col_names)

        if (df_filter.empty == True):
            return error('Erro em filter_rural_and_save_df_to_csv')
        
        df = df_filter
        filter_name = filter_name + '_ASSENTAMENTO'
        df_filter = self.filter_assentamento_and_save_df_to_csv(df, df_dic, dest_folder, souce_file_name, file_year, filter_name, col_names)
        df_filter_anexos = self.filter_anexo_and_save_df_to_csv(df_filter, df_dic, dest_folder, souce_file_name, file_year, filter_name, col_names)
             
    def filter_anexo_and_save_df_to_csv(self, df: pd.DataFrame, df_dic: pd.DataFrame, dest_folder, souce_file_name, file_year, filter_name, col_names) -> pd.DataFrame:       
        # Apenas as escolas que são anexos
        # usar comparcao dentro do df[comparacao]         
        # comparacao ...  not(~) df['CO_ESCOLA_SEDE_VINCULADA'].isnull()
        df_filter = df[~ df['CO_ESCOLA_SEDE_VINCULADA'].isnull() ]
                        
        df = df_filter

        filter_name = filter_name + '_ANEXO'

        # variavel será usada para testar e para criar os dados
        append_name_list = [file_year, filter_name]

        # recuper as colunas na ordem correta
        if len(col_names) == 0:
            col_names = df_dic.Col_Name.tolist()

        # se tiver dados salva, se não retorna None
        return utils.save_dataframe_to_csv(df_filter, dest_folder, souce_file_name, append_name_list, col_names) 

    def filter_bahia_and_save_df_to_csv(self, df: pd.DataFrame, df_dic: pd.DataFrame, dest_folder, souce_file_name, file_year, filter_name, col_names) -> pd.DataFrame:       
        # Apenas as escolas da BA
        # usar comparcao dentro do df.loc[comparacao]         
        # comparacao ...  df['CO_UF'] == 29  == 'BA'
        df_filter = df.loc[ df['CO_UF'] == int(29) ]
        
        df = df_filter

        # Apenas as escolas que são ATIVAS
        # usar comparcao dentro do df.loc[comparacao]         
        # comparacao ...  df['TP_SITUACAO_FUNCIONAMENTO'] == 1
        df_filter = df.loc[ df['TP_SITUACAO_FUNCIONAMENTO'] == int(1) ]

        # variavel será usada para testar e para criar os dados
        append_name_list = [file_year, filter_name]

        # recuper as colunas na ordem correta
        if len(col_names) == 0:
            col_names = df_dic.Col_Name.tolist()

        # se tiver dados salva, se não retorna None
        return utils.save_dataframe_to_csv(df_filter, dest_folder, souce_file_name, append_name_list, col_names) 

    def filter_estadual_and_save_df_to_csv(self, df: pd.DataFrame, df_dic: pd.DataFrame, dest_folder, souce_file_name, file_year, filter_name, col_names) -> pd.DataFrame:
        # Apenas as escolas da BA e ESTADUAL
        # usar comparcao dentro do df.loc[comparacao]         
        # comparacao ...  df['TP_DEPENDENCIA'] == 2 == ESTADUAL
        df_filter = df.loc[ df['TP_DEPENDENCIA'] == int(2) ]

        # variavel será usada para testar e para criar os dados
        append_name_list = [file_year, filter_name]

        # recuper as colunas na ordem correta
        if len(col_names) == 0:
            cols_name = df_dic.Col_Name.tolist()

        # se tiver dados salva, se não retorna None
        return utils.save_dataframe_to_csv(df_filter, dest_folder, souce_file_name, append_name_list, col_names)        

    def filter_rural_and_save_df_to_csv(self, df: pd.DataFrame, df_dic: pd.DataFrame, dest_folder, souce_file_name, file_year, filter_name, col_names) -> pd.DataFrame:
        # Apenas as escolas da BA, ESTADUAL e LOCALIZADA na zona RURAL
        # usar comparcao dentro do df.loc[comparacao]         
        # comparacao ...  df['TP_LOCALIZACAO'] == 2 == RURAL
        df_filter = df.loc[ df['TP_LOCALIZACAO'] == int(2) ]

        # variavel será usada para testar e para criar os dados
        append_name_list = [file_year, filter_name]

        # recuper as colunas na ordem correta
        if len(col_names) == 0:
            cols_name = df_dic.Col_Name.tolist()

        # se tiver dados salva, se não retorna None
        return utils.save_dataframe_to_csv(df_filter, dest_folder, souce_file_name, append_name_list, col_names) 

    def filter_assentamento_and_save_df_to_csv(self, df: pd.DataFrame, df_dic: pd.DataFrame, dest_folder, souce_file_name, file_year, filter_name, col_names) -> pd.DataFrame:
        # Apenas as escolas da BA, ESTADUAL e LOCALIZADA na zona RURAL
        # usar comparcao dentro do df.loc[comparacao]         
        # comparacao ...  df['TP_LOCALIZACAO_DIFERENCIADA'] == 1 == ASSENTAMENTO
        df_filter = df.loc[ df['TP_LOCALIZACAO_DIFERENCIADA'] == int(1) ]

        # variavel será usada para testar e para criar os dados
        append_name_list = [file_year, filter_name]

        # recuper as colunas na ordem correta
        if len(col_names) == 0:
            cols_name = df_dic.Col_Name.tolist()

        # se tiver dados salva, se não retorna None
        return utils.save_dataframe_to_csv(df_filter, dest_folder, souce_file_name, append_name_list, col_names) 

    def filter_municipio_and_save_df_to_csv(self, df: pd.DataFrame, df_dic: pd.DataFrame, dest_folder, souce_file_name, file_year, filter_name, col_names) -> pd.DataFrame:
        filter_1 = param.get_filter_1(file_year)
        filter_name = filter_name + '_' + filter_1['city_name']

        # Apenas as escolas do municipio 
        # comparacao ...  df['CO_MUNICIPIO'] == city_cod
        df_filter = df.loc[ df[  filter_1['col'] ] == filter_1['cod'] ]

        filter_name = filter_name + '_' + filter_1['city_name']

        # variavel será usada para testar e para criar os dados
        append_name_list = [file_year, filter_name]

        # recuper as colunas na ordem correta
        if len(col_names) == 0:
            cols_name = df_dic.Col_Name.tolist()

        # se tiver dados salva, se não retorna None
        return utils.save_dataframe_to_csv(df_filter, dest_folder, souce_file_name, append_name_list, col_names) 