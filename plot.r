library(ggplot2)
library(sqldf)


df = read.csv("./results.csv")

average = sqldf("SELECT version, name, AVG(cpu_cycles) as cycles, COUNT(*)  as samples, AVG(scale) as scale FROM df 
      GROUP BY version, name
      ORDER BY version, cycles")


      #also plot optimized
results = sqldf("
SELECT
    b.samples,
    b.scale,
    b.name,
    b.cycles                                   AS baseline_cycles,
    o.cycles                                   AS optimized_cycles,
    b.cycles - o.cycles                        AS cycle_delta,        -- absolute gain/loss
    ROUND((b.cycles - o.cycles) / b.cycles * 100, 2) AS pct_change,   -- % faster/slower
    CASE
        WHEN b.cycles - o.cycles >= 0 THEN 'improved'
        ELSE                                   'regressed'
    END                                        AS result
FROM   average AS b
JOIN   average AS o
       ON  o.name    = b.name
      AND o.version  = 'optimized_sort' 
WHERE  b.version     = 'baseline'
ORDER  BY cycle_delta DESC;  
      ")


ggplot(results, aes(x=name , y = cycle_delta)) + 
    geom_bar(stat="identity") + 
    ylab("Cycles per value") + 
    xlab("table name") +
    theme_bw()


ggplot(results, aes(x=name , y = pct_change)) + 
    geom_bar(stat="identity") + 
    theme_bw()


# Analyze the problem cases. 

# - do weighted average?

sqldf("SELECT * from df
      WHERE name like 'Account%user_data%'
      AND version like '%sort%'
      ")

df_all = sqldf("SELECT version, SUM(cycles * samples * scale) as cycles from average GROUP BY version")

ggplot(sqldf("select * FROM df_all where version not like 'optimized'"), aes(x=version, y =cycles/1e9)) + 
    geom_bar(stat="identity") + 
    ylab("Cycles") + 
    xlab("version ") +
    theme_bw()

# analyze absorb
df = read.csv("./sort.log");

sort_average = sqldf("SELECT version, name, AVG(cpu_cycles) as cycles, COUNT(*)  as samples, AVG(scale) as scale
                     FROM df 
                     WHERE scale > 0
      GROUP BY version, name
      ORDER BY version, cycles")

ggplot(sort_average, aes(x= name, y=cycles, fill = version)) + 
    geom_bar(stat="identity", position = "dodge") + 
    theme_bw()
