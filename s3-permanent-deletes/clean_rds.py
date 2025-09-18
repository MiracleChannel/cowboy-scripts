# non-nullable fields location_file & location_folder
# no airtable sync status for not synced
# some discarded records were never synced? no ID
# some duplicate objects have play activity


import csv
from core.models import AirtableModel
from content.models import Vid
from activity.models import PlayActivity

# UNLINK from S3 and Airtable
file_name = "20250905-Videos-Discarded-HardDeletes.csv"
ids = []
with open(file_name, newline="") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        id_value = row["id"].strip()
        if id_value.isdigit():
            ids.append(int(id_value))

len(ids)  # should be 2744

vids = Vid.objects.filter(id__in=ids)
vids.count()  # is 2742, 2 videos dont exist in rds or are duplicated on the csv

vids.filter(is_live=True).count()  # 10 videos are live, should be 0

vids.filter(is_live=False).update(
    airtable_record="",
    sync_status=AirtableModel.SYNCED,
    location_file="NONE",
    location_folder="NONE"
)

# DELETE
file_name = "20250905-Videos-Duplicates-SoftDeletes.csv"
ids = []
with open(file_name, newline="") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        id_value = row["id"].strip()
        if id_value.isdigit():
            ids.append(int(id_value))

len(ids)  # should be 290

vids = Vid.objects.filter(id__in=ids)
vids.count()  # should be 290
vids.filter(is_live=True).count()  # should be 0

PlayActivity.objects.filter(video_id__in=ids).count()  # 65, should be 0
no_del_ids = []
for pa in PlayActivity.objects.filter(video_id__in=ids):
    no_del_ids.append(pa.video_id)

len(no_del_ids)  # 65

safe2del = [id_ for id_ in ids if id_ not in no_del_ids]
len(safe2del)  # 260, 30 unique videos have play activity

vids = Vid.objects.filter(id__in=safe2del)
vids.count()  # should be 260
vids.filter(is_live=True).count()  # should be 0

vids.delete()

# Airtable
# 3035 records deleted
