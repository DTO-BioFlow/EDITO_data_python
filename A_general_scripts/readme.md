# EDITO general scripts
## Overview of scripts

| Script Name                          	| Author        		             | Description									 	|
|-------------------------------------- |---------------------------------------------------|-------------										|
| 0_list_collections.ipynb              | [Willem Boone](https://github.com/willem0boone)   | List all collections from the EDITO STAC, this can help you in navigating the STAC.	|
| 0_collection_ids.txt                  | [Willem Boone](https://github.com/willem0boone)   | Output form ```0_list_collections.ipynb```             					|
| 1_Retrieve-CMEMS-dataset.ipynb        | [Willem Boone](https://github.com/willem0boone)   | Find a CMEMS product in the STAC and open the data. 			             	|
| 2_Retrieve-eurobis-dataset.ipynb      | [Willem Boone](https://github.com/willem0boone)   | Find the Eurobis occurrence product and read \& filter the parquet file.		        |
| 3_search_biooracle_stac_zarr_assets.py| [Samuel Fooks](https://github.com/samuelfooks )   | Connects to a static json STAC and gets ARCO datasets (assets).             |
| 4_search_edito_stac_zarr_assets.ipynb | [Samuel Fooks](https://github.com/samuelfooks )   | Search a STAC conformant API for ARCO datasets (assets).           |
| 5_subsetting_arco_data.ipynb          | [Samuel Fooks](https://github.com/samuelfooks )   | Open the CSV file containing the URLs of the ARCO assets.  Open a few ARCO datasets and interactively subset the dataset based on the variables, dimensions, and time range of interest.  Then plot the subsetted rasters to do basic visual analysis             |
| 6_stac_metadata_search.py             | [Samuel Fooks](https://github.com/samuelfooks )   | Simple, client-side substring search over the EDITO STAC API: list/filter collections, scan `/collections/{id}/items`, and report matching items + matching assets (by key/title/href). Can save results as JSON/CSV. |
| 6_stac_metadata_search.sh             | [Samuel Fooks](https://github.com/samuelfooks )   | Bash version of the same workflow (uses `curl` + `jq`) with optional collection filtering, pagination controls, and JSON/CSV output (see `data/stac_metadata_search/` for example outputs). |

## Videos
### STAC browser
The STAC browser is a useful GUI tool that allows users to visualy search the STAC catalog. Watch the demo video below to learn more.<br>
[![Watch the video](https://img.youtube.com/vi/ZJHcB2fwkIQ/0.jpg)](https://www.youtube.com/watch?v=ZJHcB2fwkIQ)

### 1. Retrieve-CMEMS-dataset.ipynb
Find a dataset from CMEMs using pystac_client. The dataset is offered as ZARR. The ZARR will be sliced and plotted. 
- [Notebook](https://github.com/willem0boone/Edito_python_STAC/blob/main/Retrieve-CMEMS-dataset.ipynb) <br>
[![Watch the video](https://img.youtube.com/vi/vT4fBrzsFPk/0.jpg)](https://www.youtube.com/watch?v=vT4fBrzsFPk)

### 2. Retrieve-eurobis-dataset.ipynb
Find occurrence data from eurobis. The dataset is offered as parquet. The parquet is sliced and plotted.
- [Notebook](https://github.com/willem0boone/Edito_python_STAC/blob/main/Retrieve-eurobis-dataset.ipynb) <br>
[![Watch the video](https://img.youtube.com/vi/jpJDoB--7DU/0.jpg)](https://www.youtube.com/watch?v=jpJDoB--7DU)

