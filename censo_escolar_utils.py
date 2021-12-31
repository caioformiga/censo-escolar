from logging import error

from pandas.core.frame import DataFrame
import settings 
import interfaces

# pip install pandas
import pandas as pd

# pip install ipython
from IPython.display import display

from os import mkdir, listdir
from os.path import exists, join, isfile

# constantes
MSG_FIM = 'Finalizou!'


def create_default_folders():
    if (exists(settings.DATA_PATH) == False):
        # usa a função definida neste arquivo
        create_dir(settings.DATA_PATH)

    if (exists(settings.CENSO_APP_DATA_PATH) == False):
        # usa a função definida neste arquivo        
        create_dir(settings.CENSO_APP_DATA_PATH)

    if (exists(settings.DICTIONARY_PATH) == False):
        # usa a função definida neste arquivo
        create_dir(settings.DICTIONARY_PATH)
    
    if (exists(settings.CENSO_APP_DICTIONARY_PATH) == False):
        # usa a função definida neste arquivo
        create_dir(settings.CENSO_APP_DICTIONARY_PATH)


def create_dir(dir_path, dir_name = ''):
    path = join(dir_path, dir_name)
    mkdir(path)
    msg = f'Diretório {dir_name} criado em: {dir_path}'.strip()
    print(msg)
       

def get_file_path_list(file_folder):
    # lista de arquivos .CSV
    file_path_list = []    
    # lista de item no diretório (arquivos e sub_diretórios)
    item_name_list = listdir(file_folder)
    for item_name in item_name_list:
        # se não for file eh f        
        item_path = join(file_folder, item_name)
        if (isfile(item_path) == True):
            # armazena apenas os arquivos que são do tipo .CSV
            if (item_name.find('.CSV') != -1):
                file_path_list.append(item_path)
        
    return file_path_list


def get_dest_file_path(dest_folder, dest_file_name, append_name_list):
    
    if (len(append_name_list) > 0):
        # ex: DOCENTES_NORDESTE
        file_only_name = str(dest_file_name.split('.')[0]).strip().upper()
        # ex: CSV
        file_extension = str(dest_file_name.split('.')[1]).strip().upper()

        append_name = ''
        for n in append_name_list:
            name = str(n)
            if (len(append_name) > 0):            
                name = name.strip().upper()
                append_name = append_name + '_' + name
            else:
                append_name = name

        dest_file_name = file_only_name + '_' + append_name + '.' + file_extension
    
    if settings.CENSO_APP_NAME in dest_folder.upper():
        dest_file_name = dest_file_name
    else:
        dest_file_name = settings.CENSO_APP_NAME + '_' + dest_file_name 

    dest_file_path = join(dest_folder, dest_file_name)
    dest_file_path = r''+dest_file_path
    return dest_file_path


def find_header_row(df: pd.DataFrame):
    # contador
    i = 0
    # representa a coluna que possui o nome das variaveis
    col_index = 1
    for row in df.itertuples():
        # recupera cada a coluna [1] na linha [i] 
        tupla = row[col_index]
        
        # se a tupla não for NaN None NaT tem valor 
        # ou seja é o header do df
        if (pd.notna(tupla) == True):
            return i
        else:
            i = i + 1


def find_first_valid_row(df: pd.DataFrame, header_row, total_rows):
    # primeira linha depois do cabeçalho
    begin_row = header_row + 1
   
    # faz um slice do df 
    # linhas (begin_row) até (total_rows)
    # todas as colunas
    df = df.iloc[begin_row:total_rows]
    
    # contador começa com 
    i = begin_row
    # representa a coluna que possui o nome das variaveis
    col_index = 1
    for row in df.itertuples():
        # recupera cada a coluna [1] na linha [i] 
        tupla = row[col_index]

        # se a tupla não for NaN None NaT tem valor
        # ou seja é a linha valida
        if (pd.notna(tupla) == True):
            return i
        else:
            i = i + 1
    

def read_excel(source_file_path, sheet_index: int) -> pd.DataFrame:
    msg = f'Carregar excel: {source_file_path} planilha: {sheet_index} para a memória...'
    print(r''+msg)
    df = pd.read_excel(source_file_path, header=0, index_col=0, engine='openpyxl', sheet_name=sheet_index)
    print(r''+MSG_FIM)
    return df


def read_csv(source_file_path, col_names=[], add_col_names=[]) -> pd.DataFrame:
    msg = f'Carregar csv: {source_file_path} para a memória...'
    print(r''+msg)
    # col_names define a ordem
    if (len(col_names) > 0):
        df = pd.read_csv(source_file_path, encoding='latin1', sep='|', low_memory=False, index_col=False, header=0, usecols=col_names)
        df = df[ col_names ]

    # adiciona col_names 
    if (len(add_col_names) > 0):
        df = pd.read_csv(source_file_path, encoding='latin1', sep='|', low_memory=False, index_col=False, header=None, names=add_col_names)
    

    if (len(add_col_names) == 0) and (len(col_names) == 0):
        df = pd.read_csv(source_file_path, encoding='latin1', sep='|', low_memory=False)

    print(r''+MSG_FIM)
    return df
    

def is_dataframe_at_memory(df, source_file_path: str, current_source_file_path: str) -> bool:
    source_file_path=r''+source_file_path
    #primeira vez
    if (df is None):
        return False
    else:
        if (current_source_file_path == source_file_path):
            msg = f'Dados de: {source_file_path} já estão em memória...'
            print(r''+msg)
            return True
        else:
            return False


def create_many_data_files_filtered(source_file_name, context_name, serie_start_year, serie_end_year, filterObject: interfaces.FilterObject):
    current_data_file_path = ''
    current_dic_file_path = ''

    # usa a função definida neste arquivo
    # faz a criação dos diretórios de dados e de dicionários
    create_default_folders()

    current_year = serie_start_year
    while current_year < serie_end_year + 1:
        # variável que vai armazenar os dados usando o pandas
        df = None
        df_dic = None

        file_year = str(current_year)
        
        # carrega o arquivo DICTIONARIO DO REFFERIDO ANO na memoria usando o pandas
        # cada vez df já exisitr usa o mesmo arquivo
        source_dic_folder = join(settings.CENSO_APP_DICTIONARY_PATH, file_year)
        
        # define nome do arquivo
        source_dic_file_name = settings.CENSO_APP_NAME + '_DICIONARIO_' + context_name.upper() + "_" + file_year + ".CSV"
        source_dic_path = join(source_dic_folder, source_dic_file_name)

        # usa a função definida neste arquivo
        if (is_dataframe_at_memory(df_dic, source_dic_path, current_dic_file_path) == False ):
            # 
            col_names = []
            # se dic não tem nome nas colunas, precisa adicionar
            header_name = [
                'Col_Index',
                'Col_Name',
                'Col_Type',
                'Col_Size',                
            ]
            df_dic = read_csv(source_dic_path, col_names, header_name)                     
            current_dic_file_path = source_dic_path    
        
        # recuper as colunas na ordem correta
        col_names = df_dic.Col_Name.tolist()

        # cria o diretório do ano dentro de CENSO_APP_DATA_PATH
        dest_folder = settings.CENSO_APP_DATA_PATH
        if (exists(join(dest_folder, file_year)) == False):
            # usa a função definida neste arquivo
            create_dir(dest_folder, file_year)
        dest_folder = join(dest_folder, file_year)
        
        # carrega o arquivo DADOS DO REFFERIDO ANO na memoria usando o pandas
        # cada vez df já exisitr usa o mesmo arquivo
        source_data_folder = settings.CENSO_DATA_PATH.replace('#YEAR',file_year).replace("#CONTEXT_NAME",context_name)
        source_data_path = join(source_data_folder, source_file_name)

        # usa a função definida neste arquivo
        if (is_dataframe_at_memory(df, source_data_path, current_data_file_path) == False ):
            df = read_csv(source_data_path, col_names)
            current_data_file_path = source_data_path
                
        # variável que vai armazenar os dados usando o pandas    
        filterObject.aplly_filters_and_save_df_to_csv(df, df_dic, dest_folder, source_file_name, file_year)
        
        # proximo ano
        current_year = current_year + 1


def create_many_dictionary_files(souce_file_name, serie_start_year, serie_end_year):
    # usa a função definida neste arquivo
    # faz a criação dos diretórios de dados e de dicionários
    create_default_folders()

    current_year = serie_start_year
    while current_year < serie_end_year + 1:
        # variável que vai armazenar os dados usando o pandas
        df_dic = None

        file_year = str(current_year)
        current_year = current_year + 1

        # cria o diretório onde os arquivos serão salvos 
        # usando o ano para que siga o padrão dictionary/censo_escolar/ano
        # dentro de CENSO_APP_DICTIONARY_PATH
        dict_dest_folder = settings.CENSO_APP_DICTIONARY_PATH
        if (exists(join(dict_dest_folder, file_year)) == False):  
            # usa a função definida neste arquivo
            create_dir(dict_dest_folder, file_year) 
        dict_dest_folder = join(dict_dest_folder, file_year) 

        # carrega o arquivo DO REFFERIDO ANO na memoria usando o pandas
        # cada vez df já exisitr usa o mesmo arquivo
        source_data_folder = settings.CENSO_DICTIONARY_PATH.replace('#YEAR',file_year)
        source_file_path = join(source_data_folder, souce_file_name)
        
        source_file_list = [
            settings.SOURCE_FILE_ESCOLAS, 
            settings.SOURCE_FILE_TURMAS, 
            settings.SOURCE_FILE_DOCENTES
        ]

        sheet_index_for_escola = 0
        sheet_index_for_turma = 1
        sheet_index_for_docentes = 3
        sheet_index_list = [
            sheet_index_for_escola, 
            sheet_index_for_turma, 
            sheet_index_for_docentes
        ]

        index = 0
        while (index < len(sheet_index_list)):
            # configura o display de todas as variáveis float sem nenhuma casa decimal
            pd.set_option('display.float_format', lambda x: '%0.0f' % x)

            # faz a leitura do arquivo/planilha    
            df_dic = read_excel(source_file_path, sheet_index_list[index])

            # descobre o cabeçalho e a primeira linha válida
            total_rows = df_dic.shape[0]
            header_row = find_header_row(df_dic)
            first_valid_row = find_first_valid_row(df_dic, header_row, total_rows)
            
            # faz um slice do df 
            # linhas (first_valid_row) até (total_rows)
            # colunas 0 até 4, sendo que a coluna 0 agora é index
            df_dic = df_dic.iloc[first_valid_row:total_rows, 0:4]
            
            # remover linhas que são NaN
            # para remover do próprio arquivo use o parametro inplace = True
            # do contrário cria uma nova numeração
            df_dic = df_dic.dropna(axis=0)

            # remover a coluna (axis=1) igual a index 2
            # define uma matriz apenas com a coluna de index 1 do df
            # para remover do próprio arquivo use o parametro inplace = True
            # do contrário cria uma nova numeração
            df_dic = df_dic.drop(df_dic.columns[[1]], axis = 1)

            # define o nome do arquivo de saída
            # adiciona o prefixo no nome
            dest_file_name = 'DICIONARIO_' + source_file_list[index]
            append_name_list = [file_year] 

            # se tiver dados salva, se não retorna None
            pd.reset_option('display.float_format')            
            save_dataframe_to_csv(df_dic, dict_dest_folder, dest_file_name, append_name_list, def_encoding='utf-8', add_row_index=True, add_header=False) 

            # incrementa o contador
            index = index + 1


def save_dataframe_to_csv(df: pd.DataFrame, dest_folder, dest_file_name, append_name_list, col_names=[], def_encoding='utf-8', add_row_index=False, add_header=True) -> pd.DataFrame:
    new_df = pd.DataFrame()
    
    # usa a função definida neste arquivo
    dest_file_path = get_dest_file_path(dest_folder, dest_file_name, append_name_list)
    if exists(dest_file_path):
        print(f'A criação de dados foi ignorada pois o arquivo {dest_file_path} já existe.')
        new_df = read_csv(dest_file_path)
    else:
        # recupera a qtd de linhas
        count_row = df.shape[0] 

        print(df.shape)

        # se existem dados apos aplicar filtro, gerar o arquivo csv
        if ( count_row > 0):
            # cria um arquivo CSV
            msg = f'Criando dados para o arquivo: {dest_file_path} ...'
            print(r''+msg)

            # não escreve o index da linha no CSV
            # col_names define a ordem
            if (len(col_names) > 0):
                df.to_csv(dest_file_path, sep='|', encoding=def_encoding, index=add_row_index, header=add_header, float_format='%.0f', columns=col_names)
                new_df = df[ col_names ]
            else:
                df.to_csv(dest_file_path, sep='|', encoding=def_encoding, index=add_row_index, header=add_header, float_format='%.0f')
                new_df = df
                
            print(r''+MSG_FIM)            
            print(new_df.shape)
        else:
            msg = f'Filtro resultou em df sem nenhum registro!'
            msg = r''+msg
            print(msg)
            new_df = pd.DataFrame()
    
    return new_df