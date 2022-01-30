require("pbapply");require("data.table");require("httr");require("rvest");require("quantmod")
require("PerformanceAnalytics");require("RSQLite");require("RQuantLib");require("dplyr")

# date vector from 2018-07-31 to present
REBAL = seq.Date(as.Date("2018-07-31"),Sys.Date()+30,by="1 day")
# only include business days
REBAL = REBAL[isBusinessDay(calendar = "UnitedStates/NYSE", dates=REBAL)]
# get the EOM (end of month) Dates
REBAL = REBAL[endpoints(REBAL, on="months")]

### function to get constituents from barChart
getConstituents = function(ticker){
  # page url
  pg <- rvest::session(paste0("https://www.barchart.com/etfs-funds/quotes/",ticker,"/constituents"))
  # save page cookies
  cookies <- pg$response$cookies
  # Use a named character vector for unquote splicing with !!!
  token <- URLdecode(dplyr::recode("XSRF-TOKEN", !!!setNames(cookies$value, 
                                                             cookies$name)))
  
  request_GET <- function(x, url, ...) {
    x$response <- httr::GET(url, x$config, ..., handle = x$handle)
    x$html <- new.env(parent = emptyenv(), hash = FALSE)
    x$url <- x$response$url
    
    httr::warn_for_status(x$response)
    
    x
  }
  
  # get data by passing in url and cookies
  pg <- 
    pg %>% request_GET(
      url = paste0("https://www.barchart.com/proxies/core-api/v1/EtfConstituents?",
                   "composite=",ticker,"&fields=symbol%2CsymbolName%2Cpercent%2CsharesHeld%2C",
                   "symbolCode%2CsymbolType%2ClastPrice%2CdailyLastPrice&orderBy=percent",
                   "&orderDir=desc&meta=field.shortName%2Cfield.type%2Cfield.description&",
                   "page=1&limit=10000&raw=1"),
      config = httr::add_headers(`x-xsrf-token` = token)
    )
  
  # raw data
  data_raw <- httr::content(pg$response)
  # convert into a data table
  data <- rbindlist(lapply(data_raw$data,"[[",6), fill = TRUE, use.names = TRUE) %>% suppressWarnings()
  # subset stocks only 
  data = subset(data,data$symbolType == 1)
  # trim data frame
  data = data[,1:3]
  # format percentages
  data$percent <- as.numeric(data$percent)/100
  # sort by weight
  data = data[order(data$percent, decreasing = TRUE),]
  # return data frame
  data
}

ETF = "SPY"
const = getConstituents(ticker = ETF)
# extract tickers from ETF
tickers = const$symbol
tickers = gsub("\\.","-", tickers)

e <- new.env()
getSymbols(c(ETF,tickers), from="2018-07-01", env = e)
ALL = do.call(merge, eapply(e,Ad)); colnames(ALL) = gsub(".Adjusted","",names(ALL))
all = do.call(merge,lapply(as.list(1:ncol(ALL)),function(ii) monthlyReturn(ALL[,ii], type="arithmetic")))
colnames(all) = names(ALL) 
# alternative from DB - 
# getSymbolsDB = function(ticker){
#   dbNm = seq.Date(from=Sys.Date()-days(6), to = Sys.Date()+1, by="1 day")
#   N = which(weekdays(dbNm) == "Sunday")
#   if(length(N) == 2){dbNm=dbNm[N[2]]}else{dbNm=dbNm[N[1]]}
#   daytoday = format(dbNm,"%Y%m%d")
#   driver = dbDriver("SQLite")
#   con = dbConnect(driver, dbname = paste0("/Volumes/3TB/SQLite/",daytoday,"_getSymbols.db"))
#   data = dbGetQuery(con, paste0("SELECT * FROM getSymbols WHERE getSymbols.Symbol == '",ticker,"'"))
#   dbDisconnect(con)
#   data =  xts(data[,"Close"], order.by = as.Date(data$Date, origin="1970-01-01"))
#   colnames(data) = ticker
#   data
# }
# ALL = pblapply(as.list(c(ETF,tickers)), function(x){
#   tmp = try(getSymbolsDB(ticker=x), silent=TRUE)
#   if(!inherits(tmp,'try-error')) tmp
# })
# ALL = do.call(merge,ALL);ALL=ALL["201807::"]
# ALL = na.locf(ALL);ALL = na.locf(ALL,fromLast = TRUE)
# all = pblapply(as.list(1:ncol(ALL)),function(ii){
#   ticker = names(ALL[,ii])
#   tmp = try(monthlyReturn(ALL[,ii], type="arithmetic"),silent=TRUE)
#   if(!inherits(tmp,'try-error')){
#     colnames(tmp) = ticker
#   }else{
#       tmp = NULL
#     }
#   tmp
# })
# all = all[lapply(all, length)>0]
# all = do.call(merge,all)


# read in historical files : skip to line 127 to load up these tables
FIN = rbindlist(pblapply(as.list(REBAL), function(x){
  tmp = try(readRDS(paste0("/Volumes/6TB/FINVIZ/FINANCIALS/",format(x,"%Y%m%d"), ".rds")),silent = TRUE)
  if(!inherits(tmp, 'try-error'))
    tmp
}),use.names = TRUE,fill = TRUE)
VAL = rbindlist(pblapply(as.list(REBAL), function(x){
  tmp = try(readRDS(paste0("/Volumes/6TB/FINVIZ/VALUATION/",format(x,"%Y%m%d"), ".rds")),silent = TRUE)
  if(!inherits(tmp, 'try-error'))
    tmp
}),use.names = TRUE,fill = TRUE)

# extract data from historical values
FIN = rbindlist(pblapply(as.list(tickers),function(x){
  tmp = try(subset(FIN,FIN$Ticker == x),silent = TRUE)
  if(!inherits(tmp,'try-error'))
    tmp
}),use.names = TRUE, fill = TRUE)
VAL = rbindlist(pblapply(as.list(tickers),function(x){
  tmp = try(subset(VAL,VAL$Ticker == x),silent = TRUE)
  if(!inherits(tmp,'try-error'))
    tmp
}),use.names = TRUE, fill = TRUE)

load("quantValueSPY.RData")

# check number of unique ticker (to make sure we have complete data)
length(tickers)
length(unique(FIN$Ticker))
length(unique(VAL$Ticker))
# *******************************************************************************************
#                                 Backtest Function
# *******************************************************************************************
# ALGO: 
# For each rebalancing period long 6 stocks with:
# 1. Price-to-book Ratio (latest) < 10
# 2. Price-to-cash flow per share ratio (TTM) < 15
# 3. P/E Ratio (TTM) < 20
# 4. Rank by increasing Total Debt-to-Equity Ratio (long stocks with the lowest first)

REBAL = REBAL[REBAL < Sys.Date()]


getRETS = function(REBAL, PB, PFCF, PE,TOP){
# last DATE
LAST = length(REBAL)
# Pass in each of the rebalancing days - then rank
df = lapply(as.list(1:LAST), function(ii){
  # print(ii)
  # assign DATE
  DATE    = REBAL[ii]
  nxtDATE = REBAL[ii+1] 
  # subset valuation data.frame            
  dta = subset(VAL, VAL$Date == DATE)
  # select columns
  dta = dta[,c("Ticker","PB","PFCF","PE","Date")]
  # fill NAs
  dta[is.na(dta)] <- 100
  dta = as.data.frame(dta)
  # filter data
  dta = dta[dta$PB < PB & dta$PFCF < PFCF & dta$PE < PE,,drop=FALSE]
  if(nrow(dta) > 0){
    # add signal
    dta$sig <- 1 #as.numeric(etfPE[DATE]$sig)
    # add Total Debt-to-Equity Ratio
    dta$D2E = do.call(c,lapply(as.list(dta$Ticker), 
                               function(x) subset(FIN,FIN$Ticker == x & FIN$Date==DATE)$DebtTOEq))
    # sort in increaseing Debt-to-equity
    dta= dta[order(dta$D2E, decreasing = FALSE),]
    # add end Date (Rebalancing Date)
    dta$endDATE = nxtDATE
    
    # if filter returns more than 6 tickers, trim
    if(nrow(dta) > TOP){dta = dta[1:TOP,]}
    
    # add exit prices
    #dta$endPRC = do.call(c,lapply(as.list(1:nrow(dta)), function(ii) round(as.numeric(ALL[,dta$Ticker[ii]][dta$endDATE[ii]]),2)))
    if(dta$sig[1] == 0){dta$RET <- 0}
    if(ii == LAST){
      # add end Date (Rebalancing Date)
      dta$endDATE = index(ALL)[nrow(ALL)]
      # add exit prices  
      #dta$endPRC = do.call(c,lapply(as.list(1:nrow(dta)), function(ii) round(as.numeric(ALL[nrow(ALL),dta$Ticker[ii]]),2)))
      # add monthly Return
      if(dta$sig[1] == 0){
        dta$RET <- 0
      }else{
        dta$RET = do.call(c,lapply(as.list(1:nrow(dta)), function(ii){ 
          tmp= try(as.numeric(all[nrow(all),dta$Ticker[ii]])*dta$sig[ii],silent = TRUE)
          if(inherits(tmp,'try-error'))
          {
            tmp = 0
          }else{
            tmp
          }
          tmp
      }))
      }
    }
    if(ii < LAST){
      # add monthly Return
      dta$RET = do.call(c,lapply(as.list(1:nrow(dta)), function(ii){
        # print(ii)
        tmp = try(as.numeric(all[,dta$Ticker[ii]][dta$endDATE[ii]])*dta$sig[ii],silent = TRUE)
        if(inherits(tmp,'try-error')){tmp = 0
        }else{tmp}
        tmp
      }))
    }


  }
  as.data.frame(dta)
})
# delete empty lists
df = df[lapply(df, nrow)>0]
# as data frame
df = as.data.frame(do.call(rbind,df))
# return data frame
df
}
# test function
df  = getRETS(REBAL = REBAL, PB = 10, PFCF = 15, PE = 20, TOP=6) # IWM,SPY
# extract returns by date
RETS = rbindlist(lapply(as.list(unique(df$endDATE)),function(DATE){
  tmp = subset(df, df$endDATE == DATE)
  as.data.frame(cbind(paste(tmp$endDATE[1]),round(as.numeric(mean(tmp$RET)),4)))
}), use.names = TRUE, fill = TRUE)
colnames(RETS) = c("Date","stratRet")
# convert to xts
RETS = xts(as.numeric(RETS$stratRet), order.by = as.Date(as.character(RETS$Date), format="%Y-%m-%d"))
colnames(RETS) = "stratRET"
# get ETF returns to compare
etfRETS = all[,ETF][index(RETS)]
RETS = merge(RETS,etfRETS)
# plot returns - simple return (i.e. not reinvesting)
charts.PerformanceSummary(merge(RETS$stratRET, RETS[,2]), geometric = FALSE)
Return.annualized(RETS,geometric = FALSE)
# compounded monthly
charts.PerformanceSummary(merge(RETS$stratRET, RETS[,2]), geometric = TRUE)
Return.annualized(RETS,geometric = TRUE)

# ******************************************************************************************
#                                    Option for Optimization
# ******************************************************************************************
require("DEoptim")
rebal = REBAL
REBAL = REBAL[1:30]
toOptim = function(n)
{
  df  = getRETS(REBAL = REBAL, PB = n[1], PFCF = n[2], PE = n[3], TOP=n[4])
  if(nrow(df)>0){
  # extract returns by date
  RETS = rbindlist(lapply(as.list(unique(df$endDATE)),function(DATE){
    tmp = subset(df, df$endDATE == DATE)
    as.data.frame(cbind(paste(tmp$endDATE[1]),round(as.numeric(mean(tmp$RET)),4)))
  }), use.names = TRUE, fill = TRUE)
  colnames(RETS) = c("Date","stratRet")
  RETS = xts(as.numeric(RETS$stratRet), order.by = as.Date(as.character(RETS$Date), format="%Y-%m-%d"))
  # optimize sharpe
  OUT = -((mean(RETS)/sd(RETS))*sqrt(12))
  }else{
    OUT = 100
  }
  
  OUT
}
LOWER = c(2,3,5,1)
UPPER = c(20,20,20,15)
fnmap_f <- function(x) {c(round(x,0))}
r = DEoptim(toOptim , lower = LOWER ,upper=UPPER,fnMap = fnmap_f,control = list(itermax=100))


REBAL = rebal
df  = getRETS(REBAL = REBAL, PB = as.numeric(r$optim$bestmem[1]), 
              PFCF = as.numeric(r$optim$bestmem[2]), 
              PE = as.numeric(r$optim$bestmem[3]))
# extract returns by date
RETS = rbindlist(lapply(as.list(unique(df$endDATE)),function(DATE){
  tmp = subset(df, df$endDATE == DATE)
  as.data.frame(cbind(paste(tmp$endDATE[1]),round(as.numeric(mean(tmp$RET)),4)))
}), use.names = TRUE, fill = TRUE)

colnames(RETS) = c("Date","stratRet")

RETS = xts(as.numeric(RETS$stratRet), order.by = as.Date(as.character(RETS$Date), format="%Y-%m-%d"))
colnames(RETS) = "stratRET"

etfRETS = all[,ETF][index(RETS)]
RETS = merge(RETS,etfRETS)
charts.PerformanceSummary(merge(RETS$stratRET, RETS[,2]), geometric = FALSE)

