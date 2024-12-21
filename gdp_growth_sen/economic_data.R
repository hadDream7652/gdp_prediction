library(rsdmx)
library(dplyr)
library(Knoema)

var_eco <- as.data.frame(readSDMX("https://senegal.opendataforafrica.org/api/1.0/sdmx/data/rzjjtyd?key=1+11+12+13+14+15+2+21+22+23+24+25+26+27+28+29+3+31+32+33+34+35+36+37+38+39+310+4"))
PIB <- as.data.frame(readSDMX("https://senegal.opendataforafrica.org/api/1.0/sdmx/data/rzjjtyd?key=1"))
var_eco<- var_eco %>% select(last_col(1), last_col(0), NOMFR_INDICATOR)
var_eco <- var_eco %>% pivot_wider(names_from = NOMFR_INDICATOR, values_from = OBS_VALUE)
var_eco <- var_eco %>% select(-`PRODUIT INTERIEUR BRUT (en milliards de FCFA)`)

write_dta