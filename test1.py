from utm_tag.test_part2 import get_campaign_params

campaigns = [704011362, 704010325, 704011628, 704011482, 704011760, 704013108, 704010283, 704004623, 704002660,
             704002262, 704002942, 704004722, 704005046, 704001940]
campaigns = list(map(str, campaigns))
print(campaigns)
print(get_campaign_params(campaigns))