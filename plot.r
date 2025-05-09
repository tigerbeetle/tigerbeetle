library(ggplot2)
library(sqldf)


df = read.csv("./results.csv")

average = sqldf("SELECT version, name, AVG(cpu_cycles) as cycles FROM df 
      GROUP BY version, name
      ORDER BY version, cycles")


results = sqldf("
SELECT
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
      AND o.version  = 'optimized'
WHERE  b.version     = 'baseline'
ORDER  BY pct_change DESC;     -- or ORDER BY cycle_delta DESC for absolute wins
      ")


ggplot(results, aes(x=name , y = cycle_delta)) + 
    geom_bar(stat="identity") + 
    ylab("Cycles per value") + 
    xlab("table name") +
    theme_bw()


ggplot(results, aes(x=name , y = pct_change)) + 
    geom_bar(stat="identity") + 
    theme_bw()
