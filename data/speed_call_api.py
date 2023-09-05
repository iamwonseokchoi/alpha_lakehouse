from speed_polygon_price import PolygonPriceAPI
from speed_polygon_technicals import PolygonTechnicalsAPI
import threading


def main():
    polygon_price_api = PolygonPriceAPI()
    polygon_technicals_api = PolygonTechnicalsAPI()

    price_thread = threading.Thread(target=polygon_price_api.execute, args=(1825, 1))
    technical_thread = threading.Thread(target=polygon_technicals_api.execute, args=(1825, 1))

    price_thread.start()
    technical_thread.start()

    price_thread.join()
    technical_thread.join()


if __name__ == "__main__":
    main()
