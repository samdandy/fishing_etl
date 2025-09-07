def get_wind_speed_weight(wind_speed: str) -> int:
    wind_speed_int = int(wind_speed.split(" ")[0])
    if 0 <= wind_speed_int < 5:
        return 10
    elif 5 <= wind_speed_int < 10:
        return 9
    elif 10 <= wind_speed_int < 15:
        return 8
    elif 15 <= wind_speed_int < 20:
        return 7
    elif 20 <= wind_speed_int < 25:
        return 6
    elif 25 <= wind_speed_int < 30:
        return 5
    elif 30 <= wind_speed_int < 35:
        return 4
    elif 35 <= wind_speed_int < 40:
        return 3
    elif 40 <= wind_speed_int < 45:
        return 2
    elif 45 <= wind_speed_int < 50:
        return 1
    return 0
