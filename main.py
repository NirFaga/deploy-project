import requests
import json
import datetime
import csv
import io


theoptimizer_username = "nir-faga"
theoptimizer_password = "5gzcVE46XXyQJ!U"
airfind_api_key = "search-reporting-api-3894b4d0-24f2-11ef-8bc4-e5d79cac73af"
sheet_link = "https://docs.google.com/spreadsheets/d/1MDr6KIqRhVXccQXuysycf8gJumd39PN1I9wyxR9N2C8/export?format=csv&gid=1424025984#gid=1424025984"
fb_token = "EAALxlTB1Y9oBOZCZAi5jNanWYlOXOM3UOLJdvnY4MQySK5LB5cwg09kZB28iIst8kzVcXExHVyBgnZC9wCkUYeWYAcWoBztPrDqJxP2KS96OUmWQMlZAf281p2A60fBHmBlAzTwcswmeZAA2V0ibEYOaZAWEUykfETx9PiHiikkSfVfL18qegiIyc1H"
csv_buffer = io.StringIO()
target_days = 7
campaign_map = {}


def readExcel():
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36'
    }
    try:
        resp = requests.get(sheet_link, headers=headers).text.split('\n')
    except:
        print("Failed to open {}".format(sheet_link))
        return
    # open('airfind api.csv', mode='w+', encoding='utf-8', newline='').write(resp)
    # with open('airfind api.csv', mode='r', encoding='utf-8') as csvFile:
    reader = csv.reader(resp)
    next(reader)
    for row in reader:
        campaign_map[row[1]] = row


def findCampaignID(raw_data):
    for key, value in campaign_map.items():
        if raw_data.lower() in key.lower():
            # return ad name, ad id, impr, clicks (all), cost
            # return value[4], value[6], value[7], value[8], value[9]
            return value[0], value[1], value[2], value[3]
    return None, None, None, None


def saveData(dataset):
    global csv_buffer
    fieldnames = [
        "Date", "Type", "TrafficSourceCampaignId", "TrafficSourceWidgetId", "TrafficSourceContentId",
        "TrafficSourceSectionId", "TrafficSourceDomainId", "TrafficSourceSiteId", "TrafficSourceExchangeId",
        "TrafficSourceAdGroupId", "TrafficSourceTargetId", "TrackerCampaignId", "TrackerClicks", "TrackerConversions",
        "TrackerRevenue", "TrafficSourceImpressions", "TrafficSourceClicks", "TrafficSourceConversions", "Cost",
        "TrafficSourceRevenue", "PublisherClicks", "PublisherRevenue", "PublisherConversions"
    ]
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames,
                            delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    if len(csv_buffer.getvalue()) == 0:
        writer.writeheader()
    writer.writerow({
        "Date": dataset[0], "Type": dataset[1], "TrafficSourceCampaignId": dataset[2], "TrafficSourceWidgetId": dataset[3], "TrafficSourceContentId": dataset[4],
        "TrafficSourceSectionId": dataset[5], "TrafficSourceDomainId": dataset[6], "TrafficSourceSiteId": dataset[7], "TrafficSourceExchangeId": dataset[8],
        "TrafficSourceAdGroupId": dataset[9], "TrafficSourceTargetId": dataset[10], "TrackerCampaignId": dataset[11], "TrackerClicks": dataset[12], "TrackerConversions": dataset[13],
        "TrackerRevenue": dataset[14], "TrafficSourceImpressions": dataset[15], "TrafficSourceClicks": dataset[16], "TrafficSourceConversions": dataset[17], "Cost": dataset[18],
        "TrafficSourceRevenue": dataset[19], "PublisherClicks": dataset[20], "PublisherRevenue": dataset[21], "PublisherConversions": dataset[22]
    })


def uploadToOptimizer():
    global csv_buffer
    print("Uploading csv file to optimizer API")
    link = "https://native-api-gateway.theoptimizer.io/api/v2/usermanager/api/login"
    headers = {
        'platform': 'native',
        'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36'
    }
    data = {"username": theoptimizer_username,
            "password": theoptimizer_password}
    loaded = False
    for i in range(3):
        try:
            resp = requests.post(link, headers=headers,
                                data=json.dumps(data)).json()
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return
    if resp.get('token'):
        auth_token = resp.get('token')
    else:
        print("Auth token isn't generated for the Optimizer API, please recheck credentials and try again")
        return
    print("Authenticated on optimizer API")
    link = "https://csv-uploader.theoptimizer.io/api/upload-csv"
    headers = {
        'authorization': 'Bearer {}'.format(auth_token),
        'platform': 'native',
        'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36'
    }
    files = {
        'file': ('file', csv_buffer, 'text/csv')
    }
    loaded = False
    for i in range(3):
        try:
            resp = requests.post(link, headers=headers, files=files).json()
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return
    print(json.dumps(resp, indent=4))


def readAirFind():
    global csv_buffer
    today_start = (datetime.datetime.now() -
                   datetime.timedelta(days=target_days)).strftime("%Y-%m-%d")
    today_end = datetime.datetime.now().strftime("%Y-%m-%d")
    link = "https://api.airfind.com/report/v1?productType=search&startDate={}&endDate={}&groupBy=brand-style&apiKey={}&tz=Israel".format(
        today_start, today_end, airfind_api_key)
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36'
    }
    loaded = False
    for i in range(3):
        try:
            resp = requests.get(link, headers=headers).json()
            loaded = True
            break
        except:
            print("Failed to open {}".format(link))
    if not loaded:
        return
    try:
        convertJSONFormat(resp)
    except:
        print("Error converting JSON for {}-{}".format(today_start, today_end))
        return


def convertJSONFormat(data):
    reports = data.get('reportDetails', [{}])
    for report in reports:
        Date = report.get('date', "")
        Type = "campaign"
        TrafficSourceCampaignId = report.get(
            "brand", "") + "-" + report.get("style", "")
        TrafficSourceCampaignId = TrafficSourceCampaignId.split('-')[:-1]
        TrafficSourceCampaignId = "-".join(TrafficSourceCampaignId)
        corrected_campaign_id, fb_impr, fb_clicks_all, fb_cost = findCampaignID(
            TrafficSourceCampaignId)
        if corrected_campaign_id:
            TrafficSourceCampaignId = corrected_campaign_id
        # TrafficSourceCampaignId = "120211445250870538"
        TrafficSourceWidgetId = ""
        TrafficSourceContentId = ""
        TrafficSourceSectionId = "0"
        TrafficSourceDomainId = ""
        TrafficSourceSiteId = ""
        TrafficSourceExchangeId = ""
        TrafficSourceAdGroupId = "0"
        TrafficSourceTargetId = ""
        TrackerCampaignId = ""
        TrackerClicks = ""
        TrackerConversions = ""
        TrackerRevenue = ""
        TrafficSourceImpressions = fb_impr
        TrafficSourceClicks = fb_clicks_all
        TrafficSourceConversions = ""
        Cost = fb_cost
        TrafficSourceRevenue = ""
        PublisherClicks = max(0, int(report.get('impressions', 0)))
        PublisherRevenue = max(0, float(report.get("revenue", 0)))
        PublisherConversions = max(0, int(report.get('clicks', 0)))

        print(f"Date: {Date}")
        print(f"Type: {Type}")
        print(f"TrafficSourceCampaignId: {TrafficSourceCampaignId}")
        print(f"TrafficSourceWidgetId: {TrafficSourceWidgetId}")
        print(f"TrafficSourceContentId: {TrafficSourceContentId}")
        print(f"TrafficSourceSectionId: {TrafficSourceSectionId}")
        print(f"TrafficSourceDomainId: {TrafficSourceDomainId}")
        print(f"TrafficSourceSiteId: {TrafficSourceSiteId}")
        print(f"TrafficSourceExchangeId: {TrafficSourceExchangeId}")
        print(f"TrafficSourceAdGroupId: {TrafficSourceAdGroupId}")
        print(f"TrafficSourceTargetId: {TrafficSourceTargetId}")
        print(f"TrackerCampaignId: {TrackerCampaignId}")
        print(f"TrackerClicks: {TrackerClicks}")
        print(f"TrackerConversions: {TrackerConversions}")
        print(f"TrackerRevenue: {TrackerRevenue}")
        print(f"TrafficSourceImpressions: {TrafficSourceImpressions}")
        print(f"TrafficSourceClicks: {TrafficSourceClicks}")
        print(f"TrafficSourceConversions: {TrafficSourceConversions}")
        print(f"Cost: {Cost}")
        print(f"TrafficSourceRevenue: {TrafficSourceRevenue}")
        print(f"PublisherClicks: {PublisherClicks}")
        print(f"PublisherRevenue: {PublisherRevenue}")
        print(f"PublisherConversions: {PublisherConversions}")
        dataset = [
            Date, Type, TrafficSourceCampaignId, TrafficSourceWidgetId, TrafficSourceContentId,
            TrafficSourceSectionId, TrafficSourceDomainId, TrafficSourceSiteId, TrafficSourceExchangeId,
            TrafficSourceAdGroupId, TrafficSourceTargetId, TrackerCampaignId, TrackerClicks, TrackerConversions,
            TrackerRevenue, TrafficSourceImpressions, TrafficSourceClicks, TrafficSourceConversions, Cost,
            TrafficSourceRevenue, PublisherClicks, PublisherRevenue, PublisherConversions
        ]

        saveData(dataset)


def checkFBAPI():
    print("Checking FB API for campaign mapping")
    global campaign_map
    link = "https://graph.facebook.com/v20.0/act_357198820452350?fields=campaigns{account_id,daily_budget,effective_status,created_time,name,id,insights{clicks,conversions,cpc,cpm,ctr,date_start,date_stop,frequency,impressions,objective,quality_ranking,reach,spend}}&access_token=" + \
        fb_token + "&level=ad&time_increment=1&date_preset=last_90d&period=day"
    counter = 0
    next_page = False
    while True:
        print("Checking API {}".format(link))
        headers = {
            'accept': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
        try:
            resp = requests.get(link, headers=headers).json()
        except:
            print("Failed to open {}".format(link))
            continue
        if not next_page:
            campaign_data = resp.get('campaigns', {}).get('data', [])
            next_page = True
        else:
            campaign_data = resp.get('data', [])
        if len(campaign_data) == 0:
            print("No more campaigns found")
        counter += len(campaign_data)
        for campaign in campaign_data:
            campaign_id = campaign.get('id', '')
            campaign_name = campaign.get('name', '')
            campaign_insights = campaign.get('insights', {})
            campaign_clicks = campaign_insights.get('clicks', 0)
            campaign_impressions = campaign_insights.get('impressions', 0)
            campaign_spend = campaign_insights.get('spend', 0)
            campaign_map[campaign_name] = [
                campaign_id,
                campaign_impressions,
                campaign_clicks,
                campaign_spend
            ]
        next_cursor = resp.get('campaigns', {}).get(
            'paging', {}).get('next', '')
        if next_cursor:
            link = next_cursor
        else:
            print("Total campaigns found: {}".format(counter))
            if counter == 0:
                print("Please update fb token")
                exit(0)
            return


if __name__ == "__main__":
    # readExcel()
    checkFBAPI()
    readAirFind()
    csv_buffer.seek(0)
    if len(csv_buffer.getvalue()):
        uploadToOptimizer()
