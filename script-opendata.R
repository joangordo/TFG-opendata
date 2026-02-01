# script_llobregat.R

# 1. Carga segura de librerías
if (!require("pacman")) install.packages("pacman", repos = "https://cloud.r-project.org")
pacman::p_load(httr, jsonlite, dplyr, purrr, tidyr)

# 2. Configuración del Grid (Tu lógica original)
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
  
  grid <- expand.grid(
    lat = seq(ymin, ymax, by = step),
    lon = seq(xmin_extendido, xmax, by = step)
  ) %>% mutate(id = row_number(), localidad = "Punto grid")
  
  return(grid)
}

# 3. Función API con "paracaídas" (tryCatch)
consultar_api <- function(lat, lon) {
  url <- "https://api.open-meteo.com/v1/forecast"
  
  # Si la API falla, devolvemos NULL en lugar de romper el script
  tryCatch({
    res <- GET(url, query = list(
      latitude = lat, longitude = lon,
      hourly = "precipitation,precipitation_probability,rain,showers",
      forecast_days = 3, timezone = "Europe/Madrid", models = "best_match"
    ), timeout(20))
    
    if (status_code(res) == 200) {
      return(fromJSON(content(res, "text", encoding = "UTF-8")))
    }
  }, error = function(e) return(NULL))
  
  return(NULL)
}

# 4. Procesamiento Principal
ejecutar_prediccion_con_tiempo <- function() {
  tiempo_inicio <- Sys.time()
  grid <- obtener_grid_llobregat()
  
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
        ID = punto$id, Localidad = punto$localidad, Lat = round(punto$lat, 6), Lon = round(punto$lon, 6),
        Precip_1h  = round(sum(precip_total[1], na.rm = T), 2),
        Precip_24h = round(sum(precip_total[1:24], na.rm = T), 2),
        Precip_48h = round(sum(precip_total[1:48], na.rm = T), 2),
        Prob_Max   = as.integer(max(prob[1:48], na.rm = T)),
        Pico_Valor = round(max(precip_total[1:48], na.rm = T), 2),
        Zona_Llobregat = zona,
        Timestamp_Captura = Sys.time()
      )
    })
  
  duracion <- difftime(Sys.time(), tiempo_inicio, units = "secs")
  message(paste("⏱️ Ejecución finalizada en:", round(duracion, 2), "segundos"))
  return(resultados)
}

# --- EJECUCIÓN Y GUARDADO ---
if(!dir.exists("data")) dir.create("data")
df_final <- ejecutar_prediccion_con_tiempo()

if(!is.null(df_final)) {
  nombre_archivo <- paste0("data/prediccion_", format(Sys.time(), "%Y%m%d_%H%M"), ".csv")
  write.csv(df_final, nombre_archivo, row.names = FALSE)
  message(paste("✅ Archivo guardado:", nombre_archivo))
}
