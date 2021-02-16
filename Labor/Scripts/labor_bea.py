from DataDashboard_Windows.Modules.bea import unzip

dictionary = {
    "CAINC5N": "https://apps.bea.gov/regional/zip/CAINC5N.zip",
    "CAINC6N": "https://apps.bea.gov/regional/zip/CAINC6N.zip",
}

unzip(file_dict=dictionary)