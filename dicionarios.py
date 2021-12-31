import settings
import censo_escolar_utils as utils

# cria um arquivo de dicionário com base no arquivo do INEP (DicDados.xlsx), dentro da série de anos informada
def extract_dicionario_from_censo_escolar(start_year, end_year):
    souce_file_name = r''+settings.SOURCE_FILE_DICTIONARY

    utils.create_many_dictionary_files(souce_file_name, start_year, end_year)

# chamada inicial
serie_begin = 2016
serie_end = 2020
extract_dicionario_from_censo_escolar(serie_begin, serie_end)