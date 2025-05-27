library(ggplot2)
library(sqldf)
options(scipen=99999)


df = read.csv("./baseline_sort.csv")

df = sqldf("SELECT * FROM df WHERe element_count > 0")


ggplot(df, aes(x=table, y=cpu_cycles)) +
    geom_point() + 
    facet_grid(code ~ .)  + 
    theme_bw()


ggplot(df, aes(x=table, y=cpu_cycles * element_count)) +
    geom_point() + 
    facet_grid(code ~ .)  + 
    theme_bw()


ggplot(df, aes(x=table, y=wall_time)) +
    geom_point() + 
    facet_grid(code ~ ., scales = "free")  + 
    theme_bw()
