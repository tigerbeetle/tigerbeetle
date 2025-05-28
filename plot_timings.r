library(ggplot2)
library(sqldf)


df = read.csv("./batch_timings.csv")

ggplot(df, aes(x=as.numeric(batch_id), y=as.numeric(latency))) +
    geom_point() +
    geom_line() +
    theme_bw()

i4i = read.csv("./batch_timings_i4i8xlarge.csv")

ggplot(i4i, aes(x=as.numeric(batch_id), y=as.numeric(latency))) +
    geom_point() +
    geom_line() +
    theme_bw()


fn= read.csv("./batch_timings_fn01.csv")

ggplot(fn, aes(x=as.numeric(batch_id), y=as.numeric(latency))) +
    geom_point() +
    geom_line() +
    theme_bw()
