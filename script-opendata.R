# Cargar librerías necesarias
if (!require("pacman")) install.packages("pacman")
pacman::p_load(httr, jsonlite, dplyr, purrr, tidyr)

# 1. Configuración de parámetros y Grid
obtener_grid_llobregat <- function() {
  puntos_extremos <- list(
    sur   = c(41.2806, 1.9778),
    este  = c(41.5403, 2.2106),
    oeste = c(41.7483, 1.6058),
    norte = c(42.2950, 1.6969)
  )
  
  xmin <- min(puntos_extremos$oeste[2], puntos_extremos$sur[2])
  xmax <- max(puntos_extremos$este[2], puntos_extremos$norte[2])
  ymin <- min(puntos_extremos$sur[1], puntos_extremos$oeste[1])
  ymax <- max(puntos_extremos$norte[1], puntos_extremos$este[1])
  
  step <- 0.04
  xmin_extendido <- xmin - (6 * step)
  
  lats <- seq(ymin, ymax, by = step)
  lons <- seq(xmin_extendido, xmax, by = step)
  
  grid <- expand.grid(lat = lats, lon = lons) %>%
    mutate(id = row_number(), localidad = "Punto grid")
  
  return(grid)
}

# 2. Función para consultar la API
consultar_api <- function(lat, lon) {
  url <- "https://api.open-meteo.com/v1/forecast"
  res <- GET(url, query = list(
    latitude = lat, longitude = lon,
    hourly = "precipitation,precipitation_probability,rain,showers",
    forecast_days = 3, timezone = "Europe/Madrid", models = "best_match"
  ))
  if (status_code(res) == 200) return(fromJSON(content(res, "text", encoding = "UTF-8")))
  return(NULL)
}

# 3. Procesamiento con CRONÓMETRO
ejecutar_prediccion_con_tiempo <- function() {
  # --- INICIO DEL RELOJ ---
  tiempo_inicio <- Sys.time()
  message(paste("🚀 Iniciando proceso a las:", tiempo_inicio))
  
  grid <- obtener_grid_llobregat()
  total_puntos <- nrow(grid)
  
  resultados <- grid %>%
    split(.$id) %>%
    map_df(function(punto) {
      data_api <- consultar_api(punto$lat, punto$lon)
      if (is.null(data_api)) return(NULL)
      
      h <- data_api$hourly
      precip_total <- h$precipitation + h$rain + h$showers
      prob <- h$precipitation_probability
      
      zona <- case_when(
        punto$lat < 41.4 ~ "DELTA",
        punto$lat < 41.7 ~ "BAJO LLOBREGAT",
        punto$lat < 42.0 ~ "MEDIO LLOBREGAT",
        TRUE             ~ "ALTO LLOBREGAT/BERGUEDÀ"
      )
      
      tibble(
        ID = punto$id,
        Localidad = punto$localidad,
        Lat = round(punto$lat, 6),
        Lon = round(punto$lon, 6),
        Precip_1h  = round(sum(precip_total[1], na.rm = T), 2),
        Precip_3h  = round(sum(precip_total[1:3], na.rm = T), 2),
        Precip_6h  = round(sum(precip_total[1:6], na.rm = T), 2),
        Precip_12h = round(sum(precip_total[1:12], na.rm = T), 2),
        Precip_24h = round(sum(precip_total[1:24], na.rm = T), 2),
        Precip_48h = round(sum(precip_total[1:48], na.rm = T), 2),
        Prob_Max   = as.integer(max(prob[1:48], na.rm = T)),
        Pico_Valor = round(max(precip_total[1:48], na.rm = T), 2),
        Zona_Llobregat = zona,
        Timestamp_Captura = Sys.time()
      )
    })
  
  # --- FINAL DEL RELOJ ---
  tiempo_final <- Sys.time()
  duracion <- difftime(tiempo_final, tiempo_inicio, units = "secs")
  
  message("---------------------------------------------------------")
  message(paste("✅ Proceso completado con éxito."))
  message(paste("⏱️ Tiempo total de ejecución:", round(duracion, 2), "segundos"))
  message("---------------------------------------------------------")
  
  return(resultados)
}

# --- LOGICA DE GUARDADO ---
if(!dir.exists("data")) dir.create("data")
# --- EJECUCIÓN ---
df_final <- ejecutar_prediccion_con_tiempo()

# 4. GUARDAR EN CSV
# Nombre con Año-Mes-Dia_Hora-Minutos
nombre_archivo <- paste0("data/prediccion_", format(Sys.time(), "%Y%m%d_%H%M"), ".csv")
write.csv(df_final, nombre_archivo, row.names = FALSE)

message(paste("✅ Archivo guardado:", nombre_archivo))
