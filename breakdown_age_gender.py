# Import all the facebook mumbo jumbo
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
import datetime

import time
import os
import credential as cr
import pandas as pd

try:
    os.remove('age_gender_ads_breakdown.csv')
except OSError:
    pass

my_app_id = cr.my_app_id
my_app_secret = cr.my_app_secret
my_access_token = cr.my_access_token
account_name = cr.account_name

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

yesterdaybad = datetime.datetime.now() - datetime.timedelta(days=0)
yesterdayslash = yesterdaybad.strftime('%m/%d/%Y')
yesterdayhyphen = yesterdaybad.strftime('%m-%d-%Y')


#all the list of avaible preset in this link  :https://developers.facebook.com/docs/marketing-api/insights/parameters/v11.0
#     'time_range': {
#         'since':  "2020-11-01", 
#         'until': "2020-11-27"
#         },  

params = {
    'date_preset': 'last_7d',
    'fields': [
                AdsInsights.Field.account_id,
                AdsInsights.Field.account_name,
                AdsInsights.Field.ad_id,
                AdsInsights.Field.ad_name,
                AdsInsights.Field.adset_id,
                AdsInsights.Field.adset_name,
                AdsInsights.Field.campaign_id,
                AdsInsights.Field.campaign_name, 
                AdsInsights.Field.date_start,
                AdsInsights.Field.date_stop,
                AdsInsights.Field.impressions,
                AdsInsights.Field.inline_link_clicks,
                AdsInsights.Field.inline_post_engagement,
                AdsInsights.Field.reach,
                AdsInsights.Field.spend,
                AdsInsights.Field.unique_clicks,
                AdsInsights.Field.clicks,
                AdsInsights.Field.conversions,
                AdsInsights.Field.video_p25_watched_actions,
                AdsInsights.Field.video_p50_watched_actions,
                AdsInsights.Field.video_p75_watched_actions,
                AdsInsights.Field.video_p95_watched_actions,
                AdsInsights.Field.video_p100_watched_actions,
                AdsInsights.Field.video_thruplay_watched_actions,
                AdsInsights.Field.video_avg_time_watched_actions,
                AdsInsights.Field.unique_video_view_15_sec,
                AdsInsights.Field.unique_video_continuous_2_sec_watched_actions,
                AdsInsights.Field.actions,
                AdsInsights.Field.action_values
                

      ],
    'breakdowns': ['age','gender'],
    'level': 'ad',
    'time_increment': 1
}


#Wait till all the data is pulled 
def wait_for_async_job(async_job):
    async_job.api_get()
    while async_job[AdReportRun.Field.async_status] != 'Job Completed' or async_job[AdReportRun.Field.async_percent_completion] < 100:
        time.sleep(2)
        async_job.api_get()
        


# Iterate through the accounts
records = []
def get_data(account):
    tempaccount = AdAccount(account)

    ads = tempaccount.get_insights_async(fields = params.get('fields'), params=params) 
    wait_for_async_job(ads)
    x = ads.get_result()

    for ad in x:
        date_of_report = yesterdaybad
        account_id = ''
        account_name = ''
        ad_id = ''
        ad_name = ''
        adset_id = ''
        adset_name = ''
        campaign_id = ''
        campaign_name = ''
        age = ''
        gender = ''
        date_start = ''
        date_stop = ''
        impressions = ''
        inline_link_clicks = ''
        inline_post_engagement = ''
        reach = ''
        spend = ''
        unique_clicks = ''
        clicks = ''
        video_p25_watched_actions = ''
        video_p50_watched_actions = ''
        video_p75_watched_actions = ''
        video_p95_watched_actions = ''
        video_p100_watched_actions = ''
        video_thruplay_watched_actions = ''
        video_avg_time_watched_actions = ''
        unique_video_view_15_sec = ''
        unique_video_continuous_2_sec_watched_action = ''
        landing_page_view = ''
        view_content = ''
        complete_registration = ''
        contacts = ''
        pixel_lead = ''
        lead = ''
        add_to_cart = ''
        initiate_checkout = ''
        purchase = ''
        add_to_cart_v = ''
        initiate_checkout_v = ''
        purchase_v = ''
        comment = ''
        post_engagement = ''
        post_reaction = ''
        post_save = ''
        post_shares = ''

        if ('account_id' in ad) :
            account_id = ad['account_id']
        if ('account_name' in ad) :
            account_name = ad['account_name']
        if ('ad_id' in ad) :
            ad_id = ad['ad_id']
        if ('ad_name' in ad) :
            ad_name = ad['ad_name']
        if ('adset_id' in ad) :
            adset_id = ad['adset_id']
        if ('adset_name' in ad) :
            adset_name = ad['adset_name']
        if ('campaign_id' in ad) :
            campaign_id = ad['campaign_id']
        if ('campaign_name' in ad) :
            campaign_name = ad['campaign_name']
        if ('age' in ad) :
            age = ad['age']
        if ('gender' in ad) :
            gender = ad['gender']
        if ('date_start' in ad) :
            date_start = ad['date_start']
        if ('date_stop' in ad) :
            date_stop = ad['date_stop']
        if ('impressions' in ad) :
            impressions = ad['impressions']
        if ('inline_link_clicks' in ad) :
            inline_link_clicks = ad['inline_link_clicks']
        if ('inline_post_engagement' in ad) :
            inline_post_engagement = ad['inline_post_engagement']
        if ('reach' in ad) :
            reach = ad['reach']
        if ('spend' in ad) :
            spend = ad['spend']
        if ('unique_clicks' in ad) :
            unique_clicks = ad['unique_clicks']
        if ('clicks' in ad) :
            clicks = ad['clicks']
        if ('video_p25_watched_actions' in ad) :
            video_p25_watched_actions = ad['video_p25_watched_actions']
        if ('video_p50_watched_actions' in ad) :
            video_p50_watched_actions = ad['video_p50_watched_actions']
        if ('video_p75_watched_actions' in ad) :
            video_p75_watched_actions = ad['video_p75_watched_actions']
        if ('video_p95_watched_actions' in ad) :
            video_p95_watched_actions = ad['video_p95_watched_actions']
        if ('video_p100_watched_actions' in ad) :
            video_p100_watched_actions = ad['video_p100_watched_actions']
        if ('video_thruplay_watched_actions' in ad) :
            video_thruplay_watched_actions = ad['video_thruplay_watched_actions']
        if ('video_avg_time_watched_actions' in ad) :
            video_avg_time_watched_actions = ad['video_avg_time_watched_actions']
        if ('unique_video_view_15_sec' in ad) :
            unique_video_view_15_sec = ad['unique_video_view_15_sec']
        if ('unique_video_continuous_2_sec_watched_action' in ad) :
            unique_video_continuous_2_sec_watched_action = ad['unique_video_continuous_2_sec_watched_action']
        if ('actions' in ad):
            for action in ad['actions'] :
                if action['action_type'] == 'landing_page_view':
                    landing_page_view = action['value']
                elif action['action_type'] == 'offsite_conversion.fb_pixel_view_content':
                    view_content = action['value']
                elif action['action_type'] == 'offsite_conversion.fb_pixel_complete_registration':
                    complete_registration = action['value']
                elif action['action_type'] == 'contact_total':
                    contacts = action['value']
                elif action['action_type'] == 'offsite_conversion.fb_pixel_lead ':
                    pixel_lead = action['value']
                elif action['action_type'] == 'lead':
                    lead = action['value']
                elif action['action_type'] == 'offsite_conversion.fb_pixel_add_to_cart':
                    add_to_cart = action['value']
                elif action['action_type'] == 'offsite_conversion.fb_pixel_initiate_checkout':
                    initiate_checkout = action['value']
                elif action['action_type'] == 'offsite_conversion.fb_pixel_purchase':
                    purchase = action['value']
                elif action['action_type'] == 'comment':
                    comment = action['value']
                elif action['action_type'] == 'post_engagement':
                    post_engagement = action['value']
                elif action['action_type'] == 'post_reaction':
                    post_reaction = action['value']
                elif action['action_type'] == 'onsite_conversion.post_save':
                    post_save = action['value']
                elif action['action_type'] == 'post':
                    post_shares = action['value']
        if ('action_values' in ad):
            for action_v in ad['action_values'] :
                if action_v['action_type'] == 'offsite_conversion.fb_pixel_add_to_cart':
                    add_to_cart_v = action_v['value']
                elif action_v['action_type'] == 'offsite_conversion.fb_pixel_initiate_checkout':
                    initiate_checkout_v = action_v['value']
                elif action_v['action_type'] == 'offsite_conversion.fb_pixel_purchase':
                    purchase_v = action_v['value']


        records.append({
                'account_id' : account_id,
                'account_name' : account_name,
                'ad_id' : ad_id,
                'ad_name' : ad_name,
                'adset_id' : adset_id,
                'adset_name' : adset_name,
                'campaign_id' : campaign_id,
                'campaign_name' : campaign_name,
                'age' : age,
                'gender' : gender,
                'date_start' : date_start,
                'date_stop' : date_stop,
                'impressions' : impressions,
                'inline_link_clicks' : inline_link_clicks,
                'inline_post_engagement' : inline_post_engagement,
                'reach' : reach,
                'spend' : spend,
                'unique_clicks' : unique_clicks,
                'clicks' : clicks,
                'video_p25_watched_actions' : video_p25_watched_actions,
                'video_p50_watched_actions' : video_p50_watched_actions,
                'video_p75_watched_actions' : video_p75_watched_actions,
                'video_p95_watched_actions' : video_p95_watched_actions,
                'video_p100_watched_actions' : video_p100_watched_actions,
                'video_thruplay_watched_actions' : video_thruplay_watched_actions,
                'video_avg_time_watched_actions' : video_avg_time_watched_actions,
                'unique_video_view_15_sec' : unique_video_view_15_sec,
                'unique_video_continuous_2_sec_watched_action' : unique_video_continuous_2_sec_watched_action,
                'landing_page_view' : landing_page_view,
                'view_content' : view_content,
                'complete_registration' : complete_registration,
                'contacts' : contacts,
                'pixel_lead' : pixel_lead,
                'lead' : lead,
                'add_to_cart' : add_to_cart,
                'initiate_checkout' : initiate_checkout,
                'purchase' : purchase,
                'add_to_cart_v' : add_to_cart_v,
                'initiate_checkout_v' : initiate_checkout_v,
                'purchase_v' : purchase_v,
                'comment' : comment,
                'post_engagement' : post_engagement,
                'post_reaction' : post_reaction,
                'post_save' : post_save,
                'post_shares' : post_shares,
                'date_of_repor' : date_of_report
        })
    df= pd.DataFrame(records)
    file_name = account + '_' +'age_gender_ads_breakdown.csv'
    df.to_csv(file_name, sep = ';', index=False )

if  __name__ == "__main__":
    for k, v in account_name.items() :
        print(v)
        get_data(v)