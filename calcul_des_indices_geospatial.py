import ee
from concurrent.futures import ThreadPoolExecutor

# Initialisation de Google Earth Engine
ee.Authenticate()
ee.Initialize()

# Charger les régions
def get_regions_for_indices(regions_list):
    return ee.FeatureCollection("projects/ee-senstatpib/assets/sen_admbnda_adm1") \
        .filter(ee.Filter.inList("ADM1_FR", regions_list))

# Calcul NDVI mensuel avec agrégation par mois
def calculate_monthly_ndvi_for_region(region, start_date, end_date):
    region_name = region.get('properties').get('ADM1_FR')
    geometry = ee.Feature(region).geometry()

    # Déterminer le satellite à utiliser en fonction de l'année
    year = int(start_date.split("-")[0])
    if year < 2024:
        collection = ee.ImageCollection("MODIS/006/MOD13A2").filterDate(start_date, end_date).filterBounds(geometry).select("NDVI")
        scale = 1000  # Résolution pour MODIS
    else:
        collection = ee.ImageCollection("COPERNICUS/S2").filterDate(start_date, end_date).filterBounds(geometry) \
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20)).map(
                lambda img: img.normalizedDifference(['B8', 'B4']).rename('NDVI')
            )
        scale = 10  # Résolution pour Sentinel-2

    # Agréger par mois
    def monthly_composite(start_millis, end_millis):
        start = ee.Date(start_millis)
        end = ee.Date(end_millis)
        monthly_image = collection.filterDate(start, end).mean()

        if not monthly_image.bandNames().size().getInfo():  # Vérifie si une image existe
            return {
                "month": start.format("YYYY-MM").getInfo(),
                "ndvi_mean": None
            }

        ndvi_mean = monthly_image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geometry,
            scale=scale
        ).get("NDVI")
        return {
            "month": start.format("YYYY-MM").getInfo(),
            "ndvi_mean": ndvi_mean.getInfo() if ndvi_mean else None
        }

    # Générer la liste des mois
    start_date_ee = ee.Date(start_date)
    end_date_ee = ee.Date(end_date)
    start_dates = ee.List.sequence(
        start_date_ee.millis(),
        end_date_ee.millis(),
        30 * 24 * 60 * 60 * 1000  # Intervalle d'un mois en millisecondes
    )

    results = []
    for i in range(start_dates.size().getInfo() - 1):
        start_millis = start_dates.get(i).getInfo()
        end_millis = start_dates.get(i + 1).getInfo()
        try:
            result = monthly_composite(start_millis, end_millis)
            result["region"] = region_name
            results.append(result)
        except Exception as e:
            print(f"Erreur pour la région {region_name}, mois {start_millis}: {e}")
    return results

# Calcul Luminosité mensuelle avec agrégation par mois
def calculate_monthly_light_for_region(region, start_date, end_date):
    region_name = region.get('properties').get('ADM1_FR')
    geometry = ee.Feature(region).geometry()
    collection = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG").filterDate(start_date, end_date).filterBounds(geometry).select("avg_rad")

    # Agréger par mois
    def monthly_composite(start_millis, end_millis):
        start = ee.Date(start_millis)
        end = ee.Date(end_millis)
        monthly_image = collection.filterDate(start, end).mean()

        if not monthly_image.bandNames().size().getInfo():  # Vérifie si une image existe
            return {
                "month": start.format("YYYY-MM").getInfo(),
                "light_mean": None
            }

        light_mean = monthly_image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geometry,
            scale=500
        ).get("avg_rad")
        return {
            "month": start.format("YYYY-MM").getInfo(),
            "light_mean": light_mean.getInfo() if light_mean else None
        }

    # Générer la liste des mois
    start_date_ee = ee.Date(start_date)
    end_date_ee = ee.Date(end_date)
    start_dates = ee.List.sequence(
        start_date_ee.millis(),
        end_date_ee.millis(),
        30 * 24 * 60 * 60 * 1000  # Intervalle d'un mois en millisecondes
    )

    results = []
    for i in range(start_dates.size().getInfo() - 1):
        start_millis = start_dates.get(i).getInfo()
        end_millis = start_dates.get(i + 1).getInfo()
        try:
            result = monthly_composite(start_millis, end_millis)
            result["region"] = region_name
            results.append(result)
        except Exception as e:
            print(f"Erreur pour la région {region_name}, mois {start_millis}: {e}")
    return results

# Fonction principale pour les séries temporelles NDVI
def get_ndvi_time_series(start_date, end_date, regions_list):
    regions = get_regions_for_indices(regions_list).toList(1000).getInfo()
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(calculate_monthly_ndvi_for_region, region, start_date, end_date) for region in regions]
        for future in futures:
            try:
                results.extend(future.result())
            except Exception as e:
                print(f"Erreur NDVI Séries : {e}")
    return results

# Fonction principale pour les séries temporelles Luminosité nocturne
def get_light_time_series(start_date, end_date, regions_list):
    regions = get_regions_for_indices(regions_list).toList(1000).getInfo()
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(calculate_monthly_light_for_region, region, start_date, end_date) for region in regions]
        for future in futures:
            try:
                results.extend(future.result())
            except Exception as e:
                print(f"Erreur Luminosité Séries : {e}")
    return results
