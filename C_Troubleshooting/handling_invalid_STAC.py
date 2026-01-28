#!/usr/bin/env python3
"""
Tutorial: Retrieve and Work with CMEMS Datasets via STAC API

This script demonstrates how to:
1. Connect to the EDITO STAC catalog
2. Search for collections by variable name
3. Retrieve STAC items with error handling for invalid items
4. Filter items by product ID
5. Open and work with Zarr datasets
6. Subset and visualize data

Combines approaches from:
- CORA OA in-situ data retrieval
- CMEMS Physics Reanalysis datasets
"""

# ============================================================================
# 0. SETUP - Import required packages
# ============================================================================

from pprint import pprint
import os

# STAC and CMEMS packages
from pystac_client import Client
from pystac import Item
from copernicusmarine.core_functions import custom_open_zarr

# Visualization
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import numpy as np

# ============================================================================
# 1. CONNECT TO STAC CATALOG
# ============================================================================

print("=" * 70)
print("Step 1: Connecting to EDITO STAC API")
print("=" * 70)

STAC_API_URL = 'https://api.dive.edito.eu/data'
client = Client.open(STAC_API_URL)
print(f"Connected to: {client}\n")

# ============================================================================
# 2. EXPLORE COLLECTIONS
# ============================================================================

print("=" * 70)
print("Step 2: Loading collections")
print("=" * 70)

collections = list(client.get_collections())
print(f"Total collections available: {len(collections)}\n")

# ============================================================================
# 3. SEARCH FOR COLLECTIONS BY VARIABLE
# ============================================================================

print("=" * 70)
print("Step 3: Searching for collections by variable")
print("=" * 70)

# Define the variable to search for
# Examples:
# - "sea_water_temperature" for temperature data
# - "sea_water_salinity" for salinity data
# - "climate_forecast-sea_water_temperature" for specific forecast data
# - "ocean_mixed_layer_thickness_defined_by_sigma_theta" for IBI

variable = "sea_water_temperature"  # Change to search for different variables

print(f"Searching for collections containing: '{variable}'")
matching_collections = []
for collection in collections:
    if variable in collection.id:
        matching_collections.append(collection.id)
        print(f"  Found: {collection.id}")

if not matching_collections:
    print(f"  No collections found matching '{variable}'")
    print("  Try: 'sea_water_temperature', 'sea_water_salinity', etc.")
print()

# ============================================================================
# 4. RETRIEVE STAC ITEMS WITH ERROR HANDLING
# ============================================================================

print("=" * 70)
print("Step 4: Retrieving STAC items (with error handling)")
print("=" * 70)

items = []
invalid_items = []

# Search through matching collections
for collection_id in matching_collections:
    print(f"Processing collection: {collection_id}")
    search = client.search(collections=[collection_id])

    for item_dict in search.items_as_dicts():
        try:
            # Fix: Some items have assets as a list instead of dict
            if isinstance(item_dict.get("assets"), list):
                item_dict["assets"] = {}

            # Create Item object from dictionary
            item = Item.from_dict(item_dict)
            items.append(item)

        except Exception as e:
            # Track invalid items for debugging
            item_id = item_dict.get("id", "No ID")
            print(f"  ⚠️  Invalid item skipped: {item_id}")

            props = item_dict.get("properties", {})
            invalid_items.append({
                "collection": collection_id,
                "item_id": item_id,
                "assets_type": type(item_dict.get("assets", {})).__name__,
                "datetime": props.get("datetime"),
                "has_start": "start_datetime" in props,
                "has_end": "end_datetime" in props,
                "error": str(e),
            })

print(f"\nValid items retrieved: {len(items)}")
if invalid_items:
    print(f"Invalid items skipped: {len(invalid_items)}")
    print("\nSample of invalid items:")
    pprint(invalid_items[:3])  # Show first 3 invalid items
print()

# ============================================================================
# 5. FILTER ITEMS BY PRODUCT ID
# ============================================================================

print("=" * 70)
print("Step 5: Filtering items by CMEMS product ID")
print("=" * 70)

# Define the CMEMS product ID you're looking for
# Examples:
# - "INSITU_GLO_PHY_TS_OA_MY_013_052" (CORA OA in-situ data)
# - "GLOBAL_MULTIYEAR_PHY_001_030" (Global Ocean Physics Reanalysis)
# - "IBI_MULTIYEAR_PHY_005_002" (Atlantic-Iberian Biscay Irish)

product_id = "INSITU_GLO_PHY_TS_OA_MY_013_052"  # Change to target product

print(f"Searching for product ID: {product_id}")
print("\nFound Zarr URLs:")

found_urls = []
for item in items:
    for asset_key, asset in item.assets.items():
        if product_id in asset.href:
            # Filter for geoChunked zarr files (common for spatial analysis)
            if "geoChunked.zarr" in asset.href:
                found_urls.append(asset.href)
                print(f"  ✓ {asset.href}")

if not found_urls:
    print(f"  No Zarr URLs found for product ID: {product_id}")
    print("  Try checking other product IDs or variable names")
else:
    print(f"\nTotal Zarr URLs found: {len(found_urls)}")
print()

# ============================================================================
# 6. OPEN AND INSPECT ZARR DATASET
# ============================================================================

if found_urls:
    print("=" * 70)
    print("Step 6: Opening Zarr dataset")
    print("=" * 70)

    # Use the first geoChunked zarr URL found
    zarr_url = found_urls[0]
    print(f"Opening: {zarr_url}\n")

    try:
        # Open the Zarr dataset using copernicusmarine
        ds = custom_open_zarr.open_zarr(zarr_url)

        print("Dataset information:")
        print(f"  Dimensions: {dict(ds.dims)}")
        print(f"  Variables: {list(ds.data_vars)}")
        print(f"  Coordinates: {list(ds.coords)}")
        time_range = f"{ds.time.min().values} to {ds.time.max().values}"
        print(f"  Time range: {time_range}")
        print()

        # ============================================================================
        # 7. SUBSET DATA (OPTIONAL EXAMPLE)
        # ============================================================================

        print("=" * 70)
        print("Step 7: Subsetting data (example)")
        print("=" * 70)

        # Example: Subset for a specific region and time
        # Adjust these values for your area of interest
        lat_min, lat_max = 35, 45
        lon_min, lon_max = -15, -5

        print("Subsetting region:")
        print(f"  Latitude: {lat_min} to {lat_max}")
        print(f"  Longitude: {lon_min} to {lon_max}")

        # Get first time step for quick subset example
        if 'time' in ds.dims:
            first_time = ds.time.isel(time=0)
            subset = ds.sel(
                latitude=slice(lat_min, lat_max),
                longitude=slice(lon_min, lon_max),
                time=first_time
            )
            print(f"\nSubset shape: {dict(subset.dims)}")
            print(f"Subset variables: {list(subset.data_vars)}")
        else:
            subset = ds.sel(
                latitude=slice(lat_min, lat_max),
                longitude=slice(lon_min, lon_max)
            )
            print(f"\nSubset shape: {dict(subset.dims)}")

        print("\n✓ Dataset successfully opened and subset!")
        print("\nYou can now:")
        print("  - Access variables: subset['variable_name']")
        print("  - Plot data: subset['variable_name'].plot()")
        print("  - Export to NetCDF: subset.to_netcdf('output.nc')")

    except Exception as e:
        print(f"Error opening Zarr dataset: {e}")
        print("This might be due to:")
        print("  - Network connectivity issues")
        print("  - Authentication requirements")
        print("  - Invalid Zarr URL")

else:
    print("=" * 70)
    print("Skipping dataset opening (no Zarr URLs found)")
    print("=" * 70)
    print("\nTo continue:")
    print("1. Adjust the 'variable' search term")
    print("2. Adjust the 'product_id' filter")
    print("3. Check available collections manually")

# ============================================================================
# 8. SAVE SUBSETS AND VISUALIZATIONS
# ============================================================================

print("=" * 70)
print("Step 8: Saving subsets and creating visualizations")
print("=" * 70)

# Check if we have a dataset to work with
dataset_available = False
working_ds = None
working_url = None

# Try to use dataset from Step 6 if available
if found_urls:
    try:
        working_url = found_urls[0]
        working_ds = custom_open_zarr.open_zarr(working_url)
        dataset_available = True
        print(f"Using dataset from: {working_url}")
    except Exception as e:
        print(f"Could not open dataset from found_urls: {e}")

# If not available, try to find and open a geoChunked zarr from items
if not dataset_available:
    print("\nSearching for geoChunked zarr files in items...")
    for item in items:
        for asset_key, asset in item.assets.items():
            if product_id in asset.href and "geoChunked.zarr" in asset.href:
                try:
                    working_url = asset.href
                    working_ds = custom_open_zarr.open_zarr(working_url)
                    dataset_available = True
                    print(f"Found and opened: {working_url}")
                    break
                except Exception as e:
                    print(f"  Could not open {asset.href}: {e}")
                    continue
        if dataset_available:
            break

if dataset_available and working_ds is not None:
    print("\n" + "-" * 70)
    print("Creating subset and visualizations...")
    print("-" * 70)

    # Determine data extent - use full dataset or larger region
    if 'latitude' in working_ds.coords and 'longitude' in working_ds.coords:
        lat_data = working_ds.latitude.values
        lon_data = working_ds.longitude.values
        lat_min, lat_max = float(lat_data.min()), float(lat_data.max())
        lon_min, lon_max = float(lon_data.min()), float(lon_data.max())

        # Expand slightly for better visualization
        lat_range = lat_max - lat_min
        lon_range = lon_max - lon_min
        lat_min -= lat_range * 0.05
        lat_max += lat_range * 0.05
        lon_min -= lon_range * 0.05
        lon_max += lon_range * 0.05

        print("\nUsing full dataset extent:")
        print(f"  Latitude: {lat_min:.2f} to {lat_max:.2f}")
        print(f"  Longitude: {lon_min:.2f} to {lon_max:.2f}")
    else:
        # Fallback: larger region example
        lat_min, lat_max = 30, 50
        lon_min, lon_max = -20, 10
        print("\nUsing default region:")
        print(f"  Latitude: {lat_min} to {lat_max}")
        print(f"  Longitude: {lon_min} to {lon_max}")

    # For depth/elevation, check if it exists in the dataset
    depth_slice = None
    if 'elevation' in working_ds.dims:
        # Select surface or specific depth range
        elev_values = working_ds.elevation.values
        if len(elev_values) > 0:
            # Use surface (first elevation level, typically closest to 0)
            depth_slice = elev_values[0]
            print("\nSubset parameters:")
            print(f"  Latitude: {lat_min} to {lat_max}")
            print(f"  Longitude: {lon_min} to {lon_max}")
            print(f"  Elevation: {depth_slice} m (surface level)")

    # Create spatial subset
    subset_kwargs = {
        'latitude': slice(lat_min, lat_max),
        'longitude': slice(lon_min, lon_max)
    }

    if depth_slice is not None:
        subset_kwargs['elevation'] = depth_slice

    # Handle time dimension
    if 'time' in working_ds.dims:
        # Select first time step for quick visualization
        time_slice = working_ds.time.isel(time=0)
        subset_kwargs['time'] = time_slice
        print(f"  Time: {time_slice.values}")

    # Create the subset
    subset = working_ds.sel(**subset_kwargs)

    print("\nSubset created:")
    print(f"  Dimensions: {dict(subset.dims)}")
    print(f"  Variables: {list(subset.data_vars)}")

    # Get the first data variable for visualization
    if len(subset.data_vars) > 0:
        # Create output directory name based on product_id
        output_dir = f"output_{product_id}"
        os.makedirs(output_dir, exist_ok=True)

        # Create and save plots for each variable
        for var in subset.data_vars:
            var_subset = subset[var]

            # Remove time and elevation dimensions if present
            plot_data = var_subset
            if 'time' in plot_data.dims:
                plot_data = plot_data.isel(time=0)
            if 'elevation' in plot_data.dims:
                plot_data = plot_data.isel(elevation=0)

            # Check data statistics
            data_values = plot_data.values
            valid_data = data_values[~np.isnan(data_values)]

            if len(valid_data) == 0:
                print(f"  ⚠️  Skipping {var}: no valid data")
                continue

            print(f"\n  Variable: {var}")
            print(f"    Valid data points: {len(valid_data)}")
            print(f"    Min: {valid_data.min():.4f}, "
                  f"Max: {valid_data.max():.4f}, "
                  f"Mean: {valid_data.mean():.4f}")

            # Determine if this is point data (scatter) or gridded (contour)
            has_lat_lon = ('latitude' in plot_data.dims and
                           'longitude' in plot_data.dims)

            if has_lat_lon:
                # Create figure with cartopy projection
                fig = plt.figure(figsize=(12, 10))
                ax = plt.axes(projection=ccrs.PlateCarree())

                # Check if data is gridded (2D array) or point data (1D)
                if len(plot_data.dims) == 2:
                    # Gridded data - use pcolormesh or contour
                    lat = plot_data.latitude.values
                    lon = plot_data.longitude.values

                    # Create meshgrid if needed
                    if lat.ndim == 1 and lon.ndim == 1:
                        lon_grid, lat_grid = np.meshgrid(lon, lat)
                    else:
                        lon_grid, lat_grid = lon, lat

                    # Plot with proper handling of NaN values
                    data_2d = plot_data.values

                    # Use pcolormesh for better handling of sparse data
                    im = ax.pcolormesh(
                        lon_grid, lat_grid, data_2d,
                        cmap='viridis', transform=ccrs.PlateCarree(),
                        shading='auto')

                    # Add colorbar
                    cbar = plt.colorbar(im, ax=ax, orientation='horizontal',
                                        pad=0.05, aspect=40)
                    cbar.set_label(var, fontsize=12)

                else:
                    # Point data - use scatter plot
                    has_coords = ('latitude' in plot_data.coords and
                                  'longitude' in plot_data.coords)
                    if has_coords:
                        lat_pts = plot_data.latitude.values
                        lon_pts = plot_data.longitude.values

                        # Flatten if needed
                        if lat_pts.ndim > 1:
                            lat_pts = lat_pts.flatten()
                            lon_pts = lon_pts.flatten()
                            data_pts = data_values.flatten()
                        else:
                            data_pts = data_values

                        # Remove NaN values
                        valid_mask = ~np.isnan(data_pts)
                        lat_pts = lat_pts[valid_mask]
                        lon_pts = lon_pts[valid_mask]
                        data_pts = data_pts[valid_mask]

                        # Create scatter plot with color mapping
                        scatter = ax.scatter(
                            lon_pts, lat_pts, c=data_pts,
                            cmap='viridis', s=20, alpha=0.6,
                            transform=ccrs.PlateCarree(),
                            edgecolors='none')
                        cbar = plt.colorbar(
                            scatter, ax=ax,
                            orientation='horizontal',
                            pad=0.05, aspect=40)
                        cbar.set_label(var, fontsize=12)

                # Add coastlines and features
                ax.coastlines(resolution='50m', color='black', linewidth=0.5)
                ax.add_feature(cfeature.LAND, facecolor='lightgray',
                               alpha=0.5)
                ax.add_feature(cfeature.OCEAN, facecolor='lightblue',
                               alpha=0.3)

                # Set extent
                ax.set_extent([lon_min, lon_max, lat_min, lat_max],
                              crs=ccrs.PlateCarree())

                # Add gridlines
                gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                                  linewidth=0.5, color='gray', alpha=0.5,
                                  linestyle='--')
                gl.top_labels = False
                gl.right_labels = False
                gl.xformatter = LONGITUDE_FORMATTER
                gl.yformatter = LATITUDE_FORMATTER

                # Set title
                title = f'{var} - {product_id}'
                if 'time' in var_subset.dims:
                    time_val = var_subset.time.values[0]
                    title += f'\nTime: {time_val}'
                ax.set_title(title, fontsize=14, fontweight='bold')

                # Save the plot
                png_filename = os.path.join(output_dir, f'{var}_subset.png')
                plt.tight_layout()
                plt.savefig(png_filename, dpi=200, bbox_inches='tight')
                plt.close()
                print(f"    ✓ Saved plot: {png_filename}")
            else:
                print(f"    ⚠️  Skipping {var}: no lat/lon coordinates")

        # Save subset to NetCDF
        nc_filename = os.path.join(output_dir, f'{product_id}_subset.nc')
        subset.to_netcdf(nc_filename)
        print(f"  ✓ Saved NetCDF: {nc_filename}")

        print(f"\n✓ All outputs saved to directory: {output_dir}/")
        print(f"  - NetCDF file: {nc_filename}")
        print(f"  - PNG files: {len(subset.data_vars)} visualization(s)")

    else:
        print("\n⚠️  No data variables found in subset")

else:
    print("\n⚠️  Could not open a dataset for saving subsets.")
    print("   Make sure:")
    print("   1. The product_id matches available datasets")
    print("   2. Network connectivity is available")
    print("   3. The variable search found matching collections")

# ============================================================================
# END OF TUTORIAL
# ============================================================================

print("\n" + "=" * 70)
print("Tutorial complete!")
print("=" * 70)
print("\nNext steps:")
print("  - Modify 'variable' to search for different data types")
print("  - Modify 'product_id' to find specific CMEMS products")
print("  - Use found Zarr URLs with custom_open_zarr.open_zarr()")
print("  - Apply spatial/temporal filters as needed for analysis")
