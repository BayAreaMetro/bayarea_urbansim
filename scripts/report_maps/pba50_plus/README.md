# PBA50+ Modeling Report Maps

This directory contains scripts and data for generating supplementary map layers for the PBA50+ Forecasting and Modeling Report.

## Setup

### First-time setup:

1. **Clone the repo**
   ```bash
   git clone https://github.com/BayAreaMetro/bayarea_urbansim.git
   cd bayarea_urbansim/scripts/report_maps/pba50_plus
   ```

2. **Download large files from Box**
   - Box folder: https://mtcdrive.app.box.com/folder/363365649386
   - Download the contents of the `pba50_plus` folder
   - Copy the Box contents into your local `pba50_plus` directory

3. **Run the map preparation script**
   ```bash
   cd Python
   python prep_map_layers.py
   ```

## File Storage

- **Git**: Python scripts
- **Box**: Large data files, map outputs, ArcGIS project files
  - Link: https://mtcdrive.app.box.com/folder/363365649386

## Output

The `prep_map_layers.py` script generates map layers in the `map_data/` directory that can be loaded into ArcGIS Pro for mapping. `map_data/FBP_Discrete_Modeling_GG_v3` contains GG shapefiles that were provided by Mark Shorett, originally from [here](https://mtcdrive.app.box.com/file/1877520162335?s=ohnpzgetvztmjt1t13nzw266ph1ezyro).
