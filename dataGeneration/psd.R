library(tmvtnorm)
library(sjstats)

mu <- c(0.002, 0.004, 0.003, 0.002, 0.001, 0.003)
sigma <- matrix(c(  36, -2, -6, -1, 13, -1,
                    -2,  1, -1,  0, -1, -1,
                    -6, -1,  9,  1,  5,  0,
                    -1,  0,  1,  1, -1,  0,
                    13, -1,  5, -1, 25, -6,
                    -1, -1,  0,  0, -6,  4), 6, 6)

size <- 1000000
df <- 4
lower <- rep(-0.1, 6)
upper <- rep(0.1, 6)

# probki w ilosci size
X <- rtmvt(size, mu, sigma, df, lower, upper, algorithm = 'gibbs')
write.csv(X, file="samples.csv", row.names=FALSE)

shares <- c(0.2, 0.2, 0.2, 0.15, 0.15, 0.1)

# stopa wzrostu calego portfela
X1 <- apply(X, 1, function(x) sum(x * shares))

df <- data.frame()

#srednia
res1 = apply(X, 2, mean)
res2 = mean(X1)
res = c(res1, res2)
df = rbind(df, res)

#mediana
res1 = apply(X, 2, median)
res2 = median(X1)
res = c(res1, res2)
df = rbind(df, res)

#kwantyl rzedu 0,1
res1 = apply(X, 2, function(x) quantile(x, probs = c(0.1)))
res2 = quantile(X1, probs = c(0.1))
res = c(res1, res2)
df = rbind(df, res)

#srednia z 10% najmn.
res1 = apply(X, 2, function(x) mean(x[order(x)[1:(size / 10)]]))
res2 = mean(X1[order(X1)[1:(size / 10)]])
res = c(res1, res2)
df = rbind(df, res)

#miara bezp. 1
res1 = apply(X, 2, function(x) { 
    x_mean <- mean(x)
    x1 <-  abs(x_mean - x)
    x_mean - sum(x1) / (2 * size)
  })
x_mean <- mean(X1)
X2 <-  abs(x_mean - X1)
res2 = x_mean - sum(X2) / (2 * size)
res = c(res1, res2)
df = rbind(df, res)

#miara bezp. 2
res1 = apply(X, 2, function(x) { gmd(x) })
res2 = gmd(X1)
res = c(res1, res2)
df = rbind(df, res)

write.table(df, "stats.csv", sep=",", row.names=FALSE, col.names=FALSE)
