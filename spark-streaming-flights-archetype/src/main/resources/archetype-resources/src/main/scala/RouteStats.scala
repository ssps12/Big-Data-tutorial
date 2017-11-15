

case class RouteStats(
    originName: String,
    destinationName: String,
    clear_flights: Int = 0,
    clear_delays: Double = 0.0,
    fog_flights: Int = 0,
    fog_delays: Double = 0.0,
    rain_flights: Int = 0,
    rain_delays: Double = 0.0,
    snow_flights: Int = 0,
    snow_delays: Double = 0.0,
    hail_flights: Int = 0,
    hail_delays: Double = 0.0,
    thunder_flights: Int = 0,
    thunder_delays: Double = 0.0,
    tornado_flights: Int = 0,
    tornado_delays: Double = 0.0)
  