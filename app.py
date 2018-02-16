from challenge import TideForecastChallenge

if __name__ == "__main__":
    tcf = TideForecastChallenge()
    for d in tcf.data: print(d["query"])
