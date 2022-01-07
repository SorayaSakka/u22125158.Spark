#PRÁCTICA SPARK
pacman::p_load(httr, tidyverse,leaflet,janitor,readr,sparklyr)
url<-	"https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
httr::GET(url)
library(dplyr)
library(sparklyr)
sc<-spark_connect(master="local")

#a
#i. Limpie el/los dataset(s)
jsonlite::fromJSON(url)
ds<- jsonlite::fromJSON(url) 
ds<-ds$ListaEESSPrecio
ds<- ds %>%  as_tibble() %>% clean_names() 
ds<-ds%>% type_convert(locale = locale(decimal_mark = ","))  %>% view() %>% clean_names()
view(ds)

#ii. genere un informe y explique si encuentra alguna anomalía, en el punto ii
#iii. cree una columna nueva que deberá llamarse low-cost, y determine cuál es el precio promedio de todos los combustibles a nivel comunidades autónomas, así como para las provincias, tanto para el territorio peninsular e insular, esta columna deberá clasificar las estaciones por lowcost y no lowcost
ds_lowcost<- ds %>%mutate(low_cost=rotulo%in%c('REPSOL','CAMPSA','BP','SHELL','GALP','CEPSA')) %>% view()
view(ds_lowcost)

ds_lowcost$low_cost[ds_lowcost$low_cost==TRUE]<-"No-lowcost"
ds_lowcost$low_cost[ds_lowcost$low_cost==FALSE]<-"Lowcost"
View(ds_lowcost)

media1<- ds_lowcost %>% select(precio_bioetanol, precio_biodiesel, precio_gas_natural_comprimido,precio_gas_natural_licuado, precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, precio_gasolina_98_e5, precio_hidrogeno, rotulo, idccaa, provincia) %>% 
  group_by(idccaa) %>% summarise(media_precio_bioetanol=mean(precio_bioetanol, na.rm=TRUE), mean(precio_biodiesel, na.rm=TRUE), mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), mean(precio_hidrogeno, na.rm=TRUE)) %>% view()
view(media1)

media2<- ds_lowcost %>% select(precio_bioetanol, precio_biodiesel, precio_gas_natural_comprimido,precio_gas_natural_licuado, precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, precio_gasolina_98_e5, precio_hidrogeno, rotulo, idccaa, provincia) %>% 
  group_by(provincia) %>% summarise(media_precio_bioetanol=mean(precio_bioetanol, na.rm=TRUE), mean(precio_biodiesel, na.rm=TRUE), mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), mean(precio_hidrogeno, na.rm=TRUE)) %>% view()
view(media2)

#iv. Imprima en un mapa interactivo, la localización del top 10 mas caras y otro mapa interactivo del top 20 mas baratas
#10 mas caras (ej.:precio_biodiesel)
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_biodiesel, localidad, direccion) %>% 
  top_n(10, precio_biodiesel) %>%  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_biodiesel)

#20 mas baratas (ej.:precio_biodiesel)
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_biodiesel, localidad, direccion) %>% 
  top_n(-20, precio_biodiesel) %>%  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_biodiesel)

#v. conseguidos objetivos anteriores, debe guardar este "archivo" en una nueva tabla llamada low-cost_num_expediente 
write.csv(ds_lowcost,"C:\\Users\\soray\\Desktop\\low_cost_22125158.csv", row.names = FALSE)

#b
#i. cuántas gasolineras tiene la comunidad de Madrid y en la comunidad de Cataluña, cuántas son low-cost, cuantas no lo son
comunidad_madrid<-ds_lowcost %>% select(idccaa, low_cost, provincia) %>% filter(idccaa=="13") %>% count(low_cost)
View(comunidad_madrid)
comunidad_cataluna<-ds_lowcost %>% select(idccaa, low_cost, provincia) %>% filter(idccaa=="09") %>% count(low_cost)
View(comunidad_cataluna)

#ii. cuál es el precio promedio, el precio más bajo y el más caro de los siguientes carburantes: gasóleo A, y gasolina 95 e Premium
madrid<-ds_lowcost %>% select(idccaa, low_cost, provincia, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% filter(idccaa=="13") %>%  drop_na() %>% 
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium))
View(madrid)

cataluna<-ds_lowcost %>% select(idccaa, low_cost, provincia, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% filter(idccaa=="09") %>%  drop_na() %>% 
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium))
View(cataluna)

#iii. deberá guardar este "archivo" en una nueva tabla llamada informe_MAD_BCN_expediente
write.csv(madrid,"C:\\Users\\soray\\Desktop\\informe_MAD_22125158.csv", row.names = FALSE)
write.csv(cataluna,"C:\\Users\\soray\\Desktop\\informe_BCN_22125158.csv", row.names = FALSE)

#c
#i. conocer a nivel municipios, cuántas gasolineras son low-cost, cuantas no lo son, cuál es el precio promedio, el precio más bajo y el más caro de los siguientes carburantes: gasóleo A, y gasolina 95 e5 Premium , en todo el TERRITORIO NACIONAL, exceptuando las grandes CIUDADES ESPAÑOLAS ("MADRID", "BARCELONA", "SEVILLA" y "VALENCIA")
no_big_cities <- ds_lowcost %>% select(idccaa, low_cost, precio_gasoleo_a, precio_gasolina_95_e5_premium, municipio, id_municipio) %>% group_by(municipio, low_cost) %>% 
  filter(!municipio %in% c("Madrid", "Barcelona", "Sevilla", "Valencia")) %>% summarise(mean(precio_gasoleo_a, na.rm = TRUE), mean(precio_gasolina_95_e5_premium, na.rm = TRUE), max(precio_gasoleo_a, na.rm = TRUE), max(precio_gasolina_95_e5_premium, na.rm = TRUE), min(precio_gasoleo_a, na.rm = TRUE), min(precio_gasolina_95_e5_premium, na.rm = TRUE))
View(no_big_cities)

no_big_cities1<-no_big_cities %>% group_by(low_cost) %>% count(low_cost)
View(no_big_cities1)

#ii. deberá guardar este "archivo" en una nueva tabla llamada informe_no_grandes_ciudades_expediente
write.csv(no_big_cities,"C:\\Users\\soray\\Desktop\\informe_no_big_cities_22125158.csv", row.names = FALSE)
write.csv(no_big_cities1,"C:\\Users\\soray\\Desktop\\informe_no_big_cities1_22125158.csv", row.names = FALSE)

#d
#i. que gasolineras se encuentran abiertas las 24 horas exclusivamente, genere una nueva tabla llamada no_24_horas sin la variable horario
no_24_horas<-ds %>% select(c_p,direccion,localidad,municipio,id_provincia,ideess) %>% 
  filter(ds$horario=="L-D: 24H")
View(no_24_horas)

#ii. deberá guardar este "archivo" en una nueva tabla llamada no_24_horas
write.csv(no_24_horas,"C:\\Users\\soray\\Desktop\\no_24_horas_22125158.csv", row.names = FALSE)

#e
#i. deberá añadir la población al dataset original creando una nueva columna denominada población (a nivel municipal)

#ii. desde esta ultima calcule cuanta competencia (nombre de la gasolinera y direccion) tiene en:
#1.En un radio de 1 km (genere mapa_competencia1.html)
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_98_e5, localidad, direccion, ideess) %>% 
  leaflet() %>% addTiles() %>%
  addCircles(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~ideess, radius = 1)  
#2.En un radio de 2 km (genere mapa_competencia2.html)
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_98_e5, localidad, direccion, ideess) %>% 
  leaflet() %>% addTiles() %>%
  addCircles(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~ideess, radius = 2)
#3.En un radio de 4 km (genere mapa_competencia3.html)
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_98_e5, localidad, direccion, ideess) %>% 
  leaflet() %>% addTiles() %>%
  addCircles(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~ideess, radius = 4)

#iii. genere el TopTen de municipios entre todo el territorio nacional excepto el territorio insular, donde no existan gasolineras 24 horas, agrupadas entre low-cost y no low-cost, deberá guardar este "archivo" en una nueva tabla llamada informe_top_ten_expediente
