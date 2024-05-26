def get_iqr_bounds(data, col, threshold=1.5):
    import pandas as pd
    IQR = data[col].quantile(0.75) - data[col].quantile(0.25)
    up_bound = data[col].quantile(0.75) + (IQR * threshold)
    low_bound = data[col].quantile(0.25) - (IQR * threshold)

    return up_bound, low_bound


# def check_is_inside_mkad(lat, lon, center, radius):
#     from geopy.distance import geodesic
#     point = (lat, lon)
#     distance = geodesic(center, point).kilometers
#     return distance <= radius