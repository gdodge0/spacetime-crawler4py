import re
from urllib.parse import urlsplit, parse_qsl, urlunsplit, urlencode
import validators


date_regex = re.compile(r"\b(?:\d{4}[-/.](?:0?[1-9]|1[0-2])[-/.](?:0?[1-9]|[12]\d|3[01])|(?:0?[1-9]|1[0-2])[-/.](?:0?[1-9]|[12]\d|3[01])[-/.]\d{2,4}|(?:0?[1-9]|[12]\d|3[01])[-/.](?:0?[1-9]|1[0-2])[-/.]\d{2,4}|\d{8})\b")
year_month_regex = re.compile(r"\d{4}[-/.](?:0?[1-9]|1[0-2])")

def is_date(date_str: str) -> bool:
    return bool(date_regex.fullmatch(date_str))

def is_year_month(s: str) -> bool:
    return bool(year_month_regex.fullmatch(s))

def is_large(string: str) -> bool:
    if len(string) >= 128:
        return True
    else:
        return False


def pattern_detection(path: str) -> str:
    path_split = path.split("/")
    # Identify files
    leaf_idx = -1
    for i, item in enumerate(path_split):
        if item:
            leaf_idx = i
    rebuilt_path = ""
    for i, item in enumerate(path_split):
        if item == "":
            rebuilt_path += "/"
        elif item.isnumeric():
            rebuilt_path += "{NUMBER}/"
        elif is_date(item):
            rebuilt_path += "{DATE}/"
        elif is_year_month(item):
            rebuilt_path += "{YEAR_MONTH}/"
        elif is_large(item):
            rebuilt_path += "{LARGE}/"
        elif ":" in item:
            parts = item.split(":")
            for ns in parts[:-1]:
                rebuilt_path += ns.lower() + "/"
            rebuilt_path += "{PAGE}/"
        elif i == leaf_idx and "." in item:
            rebuilt_path += "{FILE}/"
        else:
            rebuilt_path += item.lower() + "/"

    rebuilt_path = rebuilt_path.rstrip("/")

    return rebuilt_path


_APACHE_SORT_VALUE = re.compile(r"^[A-Za-z](?:;[CO]=[A-Za-z])*$")


def is_apache_sort_query(query: list[tuple[str, str]]) -> bool:
    # Apache sort evades parse_qsl, so we're handling it explicitly
    if not query:
        return False
    for k, v in query:
        if k not in ("C", "O"):
            return False
        if not _APACHE_SORT_VALUE.match(v):
            return False
    return True


def strip_query(query: list[tuple[str, str]]) -> list[tuple[str, str]]:
    # from: https://raw.githubusercontent.com/AdguardTeam/AdguardFilters/master/TrackParamFilter/sections/general_url.txt
    # removed regex entries
    bad_queries = {
        'link_source', 'taid', 'tgclid', 'analytics_context', 'analytics_trace_id', 'bance_xuid', 'elqTrackId', 'elq', 'elqaid', 'elqat', 'elqCampaignId', 'elqak', 'winflncrtag', 'ftag', 'janet', 'vs_campaign_id', 'adsterra_clid', 'adsterra_placement_id', 'loclid', 'ldtag_cl', 'lt_r', 'srclt', '_ly_c', '_ly_r', 'yj_r', 'line_uid', 'mt_click_id', 'mt_network', 'mt_campaign', 'mt_adset', 'mt_creative', 'mt_medium', 'mt_sub1', 'mt_sub2', 'mt_sub3', 'mt_sub4', 'mt_sub5', 'adc_publisher', 'adc_token', 'tw_medium', 'tw_profile_id', 'tw_source', 'uzcid', 'beyond_uzcvid', 'beyond_uzmcvid', 'srsltid', '_sgm_campaign', '_sgm_term', '_sgm_pinned', 'ebisOther1', 'ebisOther2', 'ebisOther3', 'ebisOther4', 'ebisOther5', 'bemobdata', '_bhlid', '_bdadid', 'recommended_by', 'recommended_code', 'personaclick_search_query', 'personaclick_input_query', 'ems_dl', 'emcs_t', 'cstrackid', 'btag', 'jmtyClId', 'Tcsack', 'vsm_type', 'vsm_cid', 'vsm_pid', 'cjdata', 'cjevent', 'at_campaign', 'at_campaign_type', 'at_creation', 'at_emailtype', 'at_link', 'at_link_id', 'at_link_origin', 'at_link_type', 'at_medium', 'at_ptr_name', 'at_recipient_id', 'at_recipient_list', 'at_send_date', '_ope', 'af_xp', 'af_ad', 'af_adset', 'af_click_lookback', 'af_force_deeplink', 'is_retargeting', 'dclid', 'sms_click', 'sms_source', 'sms_uph', 'ttclid', 'spot_im_redirect_source', 'mt_link_id', 'iclid', 'user_email_address', '_gl', 'cm_me', 'cm_cr', 'sscid', 'rtkcid', 'ir_campaignid', 'ir_adid', 'ir_partnerid', '__io_lv', '_io_session_id', 'asgtbndr', 'ymid', 'gci', 'pk_vid', 'mindbox-click-id', 'famad_xuid', 'twclid', 'cx_click', 'cx_recsOrder', 'cx_recsWidget', 'mkt_tok', 'mindbox-message-key', 's_cid', 'adobe_mc_ref', 'adobe_mc_sdid', 'awc', '_hsmi', '__hsfp', '__hssc', '__hstc', '_hsenc', 'hsa_acc', 'hsa_ad', 'hsa_cam', 'hsa_grp', 'hsa_kw', 'hsa_la', 'hsa_mt', 'hsa_net', 'hsa_ol', 'hsa_src', 'hsa_tgt', 'hsa_ver', 'hsCtaTracking', 'ysclid', 'yclid', 'aiad_clid', '_sgm_campaign', '_sgm_source', '_sgm_action', 'mc_cid', 'mc_eid', 'maf', '_clde', '_cldee', 'wt_mc', 'oprtrack', 'xtor', 'msclkid', 'vero_conv', 'vero_id', 'int_content', 'int_term', 'int_source', 'int_medium', 'int_campaign', 'itm_source', 'itm_medium', 'itm_campaign', 'itm_content', 'itm_term', 'utm_compaign', 'utm_prid', 'utm_wave', 'utm_lob', 'utm_emailid', 'utm_email', 'utm_newsletterid', 'utm_tag', 'utm_adset', 'utm_adgroup', 'utm_ad', 'utm_affiliate', 'utm_brand', 'utm_campaignid', 'utm_channel', 'utm_cid', 'utm_creative', 'utm_emcid', 'utm_emmid', 'utm_id', 'utm_id_', 'utm_keyword', 'utm_name', 'utm_place', 'utm_product', 'utm_pubreferrer', 'utm_reader', 'utm_referrer', 'utm_serial', 'utm_servlet', 'utm_session', 'utm_siteid', 'utm_social', 'utm_social-type', 'utm_source_platform', 'utm_supplier', 'utm_swu', 'utm_umguk', 'utm_userid', 'utm_viz_id', 'utm_source_code', 'utm_campaign_name', 'utm_journey_id', 'gad_campaignid', 'gad_source', 'gbraid', 'wbraid', 'gclsrc', 'gclid', 'usqp', 'dpg_source', 'dpg_campaign', 'dpg_medium', 'dpg_content', 'admitad_uid', 'adj_label', 'adj_campaign', 'adj_creative', 'gps_adid', 'unicorn_click_id', 'adjust_creative', 'adjust_tracker_limit', 'adjust_tracker', 'adjust_adgroup', 'adjust_campaign', 'adjust_referrer', 'external_click_id', 'bsft_clkid', 'bsft_eid', 'bsft_mid', 'bsft_uid', 'bsft_aaid', 'bsft_ek', 'mtm_campaign', 'mtm_cid', 'mtm_content', 'mtm_group', 'mtm_keyword', 'mtm_medium', 'mtm_placement', 'mtm_source', 'pk_campaign', 'pk_medium', 'pk_source', '_branch_referrer', '_branch_match_id', 'ml_subscriber', 'ml_subscriber_hash', 'rb_clickid', 'oly_anon_id', 'ebisAdID', 'wickedid', 'irgwc', 'fbclid', 'fbadid', 'nb_placement', 'nb_expid_meta', 'adfrom', 'nx_source', '_zucks_suid', 'guccounter', 'guce_referrer', 'guce_referrer_sig', '_openstat', 'action_object_map', 'action_ref_map', 'action_type_map', 'fb_action_ids', 'fb_action_types', 'fb_comment_id', 'fb_ref', 'fb_source', 'cmpid', 'adj_t', 'cuid', 'a8', 'utm_medium', 'utm_campaign', 'utm_referrer', 'utm_content', 'utm_source', 'utm_term', 'irclickid', 'vc_lpp', 'erid', 'oly_enc_id', '_ga', 'tduid',
                   'do', 'idx', 'rev', 'image', 'ns', 'at', 'media', 'tab_files', 'tab_details', 'mode', 'sectok', # docuwiki
                   'version', 'action', 'format', # trac and other wiki/CMS versioning params
                   'tribe-bar-date', 'ical', 'outlook-ical'  # observed bad headers on WICS
                   }

    return [(k, v) for k, v in query if k not in bad_queries]


def normalize_url(url: str) -> dict | None:
    # Step 1: Check if the url is valid to begin with
    if not validators.url(url):
        return {
            "fetch_url": None,
            "dedup_key": None,
            "bucket_keys": [],
            "normalized_urlsplit": {
                "scheme": None,
                "netloc": None,
                "path": None,
                "query": None,
            }
        }

    # Step 2: split url
    split = urlsplit(url)

    scheme = split.scheme

    netloc = split.netloc.split(":")[0]
    netloc = netloc.lower() # normalize to lowercase
    netloc = netloc.rstrip("/") # remove trailing slashes

    # Strip leading www. for dedup ke
    dedup_netloc = netloc[4:] if netloc.startswith("www.") else netloc

    path = split.path

    dedup_path = path.rstrip("/") or "/"

    query = parse_qsl(split.query)
    query = strip_query(query) # remove ad / junk query params
    if is_apache_sort_query(query):
        query = []
    query = sorted(query) # normalize query param order
    query = urlencode(query)

    return {
        "fetch_url": urlunsplit((scheme, netloc, path, query, "")),
        "dedup_key": urlunsplit(("http", dedup_netloc, dedup_path, query, "")),
        "bucket_keys": [pattern_detection(path)],
        "normalized_urlsplit": {
            "scheme": scheme,
            "netloc": dedup_netloc,
            "path": path,
            "query": query,
        }
    }



