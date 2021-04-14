from bea import bea

dictionary = {
    "CAINC5N": "https://apps.bea.gov/regional/zip/CAINC5N.zip",
    "CAINC6N": "https://apps.bea.gov/regional/zip/CAINC6N.zip",
}

bea(file_dict=dictionary, file_location="Labor")
