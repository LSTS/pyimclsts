from copernicusmarine import CopernicusMarineClient

client = CopernicusMarineClient()

client.subset(
    dataset_id="GLOBAL_ANALYSISFORECAST_PHY_001_024",
    variables=["thetao"],                  # temperature variable
    minimum_longitude=-10,
    maximum_longitude=10,
    minimum_latitude=30,
    maximum_latitude=50,
    start_datetime="2024-01-01T00:00:00",
    end_datetime="2024-01-10T00:00:00",
    output_filename="sst_subset.nc"
)