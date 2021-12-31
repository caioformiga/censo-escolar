import settings 
import censo_escolar_utils as utils
import escolas_filter

# aplica os filtros em cada arquivos ESCOLAS.CSV, dentro da série de anos informada
def select_escolas_filtered_from_escolas_censo_escolar(start_year, end_year):
    context_name = r''+settings.CONTEXT_ESCOLAS
    souce_file_name = r''+settings.SOURCE_FILE_ESCOLAS

    filterObj = escolas_filter.FilterOfEscolas()
    utils.create_many_data_files_filtered(souce_file_name, context_name, start_year, end_year, filterObj)


# chamada inicial
serie_begin = 2016
serie_end = 2020
select_escolas_filtered_from_escolas_censo_escolar(serie_begin, serie_end)