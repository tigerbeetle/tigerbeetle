library(ggplot2)
library(sqldf)
options(scipen=99999)


df_std = read.csv("./baseline_sort.csv")
df_radix = read.csv("./radix_sort.csv")

df = rbind(df_std,df_radix)

df = sqldf("SELECT * FROM df WHERe element_count > 0")


ggplot(df, aes(x=table, y=cpu_cycles, color = name)) +
    #geom_point() + 
geom_violin() + 
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




sqldf("SELECT SUM(cpu_cycles), name, code FROM df GROUP BY code, name")
