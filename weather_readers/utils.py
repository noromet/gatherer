class UnitConverter:
    
    def fahrenheit_to_celsius(fahrenheit: float) -> float:
        return round((fahrenheit - 32) * 5/9, 4) if fahrenheit is not None else None

    
    def psi_to_hpa(pressure: float) -> float:
        return round(pressure * 33.8639, 4) if pressure is not None else None
    
    
    def mph_to_kph(speed: float) -> float:
        return round(speed * 1.60934, 4) if speed is not None else None

    
    def inches_to_mm(inches: float) -> float:
        return round(inches * 25.4, 4) if inches is not None else None