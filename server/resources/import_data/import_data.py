from pandas_datareader import data as pdr
import fix_yahoo_finance as yf
yf.pdr_override()

#get_data parameters
start = "2006-01-01"
end = "2018-06-27"
#euronext_lisboa
tickers = ["EDPR", "GALP", "NOS", "RENE", "BCP", "RAM", "CTT",
	"EDP", "SON", "NVG", "SEM", "ALTR", "SONC", "COR", "JMT",
	"IBS", "PHR", "EGL"]

for i in range (len(tickers)):
	tickers[i] = tickers[i]+".LS"

print(tickers)

#data = yf.download(tickers, start, end, as_panel=False, group_by="ticker", threads=4)


for t in tickers:
	#download dataframe
	data = pdr.get_data_yahoo(t, start, end, as_panel=False)
	data["Symbol"] = t
	data.to_csv("data/"+t+".csv", sep=";", encoding="utf-8")
