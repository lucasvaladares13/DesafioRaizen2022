from urllib import request



def download_file():
    file_url = 'https://github.com/raizen-analytics/data-engineering-test/blob/master/assets/vendas-combustiveis-m3.xls'
    file = '/usr/local/data/0_raw/vendas-combustiveis-m3.xls'

    request.urlretrieve(file_url , file )






