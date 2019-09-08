def col_change(cols_list):
    cols_name = {'uid_first':'uid',
                 'device_name_first':'device',
 'request_id_unique':'request_id',
 'sso_id_first':'sso_id',
 'event_time_min':'first_time',
 'event_time_max':'last_time',
  'event_time_count':'total_pages',
 'session_duration_sum':'duration',
 'article_id_nunique':'n_articles',
 'utm_medium_first':'utm_medium',
 'utm_content_first':'utm_content',
 'utm_source_first':'utm_source',
 'utm_campaign_first':'utm_campaign',
 'referrer_content_first':'referrer_content',
 'referrer_platform_first':'referrer_platform',
 'campaign_name_first':'campaign_name',
 'anonymous_id_first':'first_anonymous_id',
 'anonymous_id_nunique':'num_anonymous_id',
 'platform_first':'first_platform',
 'platform_nunique':'n_platform',
 'brand_first':'first_brand',
 'brand_last':'last_brand',
 'brand_nunique':'n_brands',
 'country_first':'country',
 'user_type_first':'first_user_type',
 'user_type_last':'last_user_type',
 'action_id_<lambda>':'registration',
 'premium_sum':'n_premium',
 'openmode_sum':'n_openmode',
 'unique_articles':'unique_articles',
 'promotions':'unique_promotion_session',
 'non_consectutive_home':'non_consectutive_home',
 'page_types_article':'page_types_article',
 'page_types_section':'page_types_section',
 'page_types_homepage':'page_types_homepage',
 'page_types_other':'page_types_other',
 'page_types_misc':'page_types_misc',
 'page_types_promotions':'page_types_promotions',
 'subscription_<lambda>':'subscription'}
    new_names = []
    for col in cols_list:
        try:
            new_names.append(cols_name[col])
        except:
            new_names.append(col)
    return new_names
