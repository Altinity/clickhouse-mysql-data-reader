#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <dump-path>"
    exit -1
fi

DUMP_PATH=$1

source tb_tables.config

###########
### titles
###########

echo "Dumping titles"
mysqldump --host=127.0.0.1 --port=3307 --user=tinybird --password=goo7eu9AeS3i --single-transaction --quick movida_preproduction titles > $DUMP_PATH/titles.sql

echo "use $TB_DATABASE;" > $DUMP_PATH/titles-insert-tb.sql
cat $DUMP_PATH/titles.sql | grep "INSERT INTO" >> $DUMP_PATH/titles-insert-tb.sql
sed -i 's/INSERT INTO `titles` VALUES/INSERT INTO `t_8a192b9c7ece4572a5a2fc9858e26d5c` (`id`, `name`, `licensor_id`, `created_at`, `updated_at`, `company_id`, `series_id`, `external_id`, `poster_file_name`, `poster_content_type`, `poster_file_size`, `poster_updated_at`, `episode_number`, `dirty_episode_number`, `rights_count`, `blackouts_count`, `denied_rights_count`, `images_count`, `cover_image_id`, `title_type`, `metadata_updated_at`, `promoted_content_id`, `promoted_content_type`, `soft_destroyed`, `credits_count`, `translated_attributes`, `rules_count`, `discarded`, `episode_reference_id`, `brand_id`) VALUES/g' $DUMP_PATH/titles-insert-tb.sql

echo "Truncate titles table"
echo "truncate $TB_DATABASE.$TITLES_TABLE" | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn

echo "Loading titles into CH"
cat $DUMP_PATH/titles-insert-tb.sql | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn
echo "Titles loaded"

read -p "Press enter to continue"

###########
### assets
###########

echo "Dumping assets"
mysqldump --host=127.0.0.1 --port=3307 --user=tinybird --password=goo7eu9AeS3i --single-transaction --quick movida_preproduction assets > $DUMP_PATH/assets.sql

echo "use $TB_DATABASE;" > $DUMP_PATH/assets-insert-tb.sql
cat $DUMP_PATH/assets.sql | grep "INSERT INTO" >> $DUMP_PATH/assets-insert-tb.sql
sed -i 's/INSERT INTO `assets` VALUES/INSERT INTO `t_4c03fdeb4e3e4db784ead40b06ec8617` (`id`, `name`, `title_id`, `created_at`, `updated_at`, `description`, `runtime_in_milliseconds`, `metadata_updated_at`, `company_id`, `asset_type_enumeration_entry_id`, `external_id`) VALUES/g' $DUMP_PATH/assets-insert-tb.sql

echo "Truncate assets table"
echo "truncate $TB_DATABASE.$ASSETS_TABLE" | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn

echo "Loading assets into CH"
cat $DUMP_PATH/assets-insert-tb.sql | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn
echo "Assets loaded"

read -p "Press enter to continue"

#######################
### Collection-entries
#######################

echo "Dumping collection-entries"
mysqldump --host=127.0.0.1 --port=3307 --user=tinybird --password=goo7eu9AeS3i --single-transaction --quick movida_preproduction collection_entries > $DUMP_PATH/collections.sql

echo "use $TB_DATABASE;" > $DUMP_PATH/collections-insert-tb.sql
cat $DUMP_PATH/collections.sql | grep "INSERT INTO" >> $DUMP_PATH/collections-insert-tb.sql
sed -i 's/INSERT INTO `collection_entries` VALUES/INSERT INTO `t_3dd7b323438943c687bd4e13a0e181a1` (`collection_id`, `title_id`, `id`, `position`) VALUES/g' $DUMP_PATH/collections-insert-tb.sql

echo "Truncate collections table"
echo "truncate $TB_DATABASE.$COLLECTIONS_TABLE" | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn

echo "Loading collection-entries into CH"
cat $DUMP_PATH/collections-insert-tb.sql | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn
echo "Collection-entries loaded"

read -p "Press enter to continue"

##############
### Features
##############

echo "Dumping features"
mysqldump --host=127.0.0.1 --port=3307 --user=tinybird --password=goo7eu9AeS3i --single-transaction --quick movida_preproduction features > $DUMP_PATH/features.sql

echo "use $TB_DATABASE;" > $DUMP_PATH/features-insert-tb.sql
read -p "Press enter to continue use"
cat $DUMP_PATH/features.sql | grep "INSERT INTO" >> $DUMP_PATH/features-insert-tb.sql
read -p "Press enter to continue insert"
sed -i 's/INSERT INTO `features` VALUES/INSERT INTO `t_23f41723e0eb480088cbb1c8f890a38c` (`id`, `name`, `enabled`, `company_id`, `created_at`, `updated_at`) VALUES/g' $DUMP_PATH/features-insert-tb.sql
read -p "Press enter to continue sed"
echo "Truncate features table"
echo "truncate $TB_DATABASE.$FEATURES_TABLE" | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn

echo "Loading features into CH"
cat $DUMP_PATH/features-insert-tb.sql | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn
echo "Features loaded"

read -p "Press enter to continue"

##############
### Platforms
##############

echo "Dumping platforms"
mysqldump --host=127.0.0.1 --port=3307 --user=tinybird --password=goo7eu9AeS3i --single-transaction --quick movida_preproduction platforms > $DUMP_PATH/platforms.sql

echo "use $TB_DATABASE;" > $DUMP_PATH/platforms-insert-tb.sql
cat $DUMP_PATH/platforms.sql | grep "INSERT INTO" >> $DUMP_PATH/platforms-insert-tb.sql
sed -i 's/INSERT INTO `platforms` VALUES/INSERT INTO `t_83f598dc74254de68216a7c7735caffb` (`id`, `company_id`, `name`, `created_at`, `updated_at`, `sequence_service_titles_url`, `_deprecated_sequence_template_name`, `_deprecated_owned`, `sequence_template_url`, `metadata_constant_name`, `outlet_id`, `automatic_publication_enabled`, `metadata_updated_at`, `granted_categories`, `external_id`, `timezone`) VALUES/g' $DUMP_PATH/platforms-insert-tb.sql

echo "Truncate platforms table"
echo "truncate $TB_DATABASE.$PLATFORMS_TABLE" | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn

echo "Loading platforms into CH"
cat $DUMP_PATH/platforms-insert-tb.sql | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn
echo "Platforms loaded"

read -p "Press enter to continue"

#################
### Schedulings
#################

echo "Dumping schedulings"
mysqldump --host=127.0.0.1 --port=3307 --user=tinybird --password=goo7eu9AeS3i --single-transaction --quick movida_preproduction schedulings > $DUMP_PATH/schedulings.sql

echo "use $TB_DATABASE;" > $DUMP_PATH/schedulings-insert-tb.sql
cat $DUMP_PATH/schedulings.sql | grep "INSERT INTO" >> $DUMP_PATH/schedulings-insert-tb.sql
sed -i 's/INSERT INTO `schedulings` VALUES/INSERT INTO `t_b5e541d4e73d4301ba736c427bd667c5` (`id`, `title_id`, `put_up`, `take_down`, `created_at`, `updated_at`, `cleared`, `platform_id`, `rule_id`, `workflow_offset`, `sequence_asset_url`, `sequence_asset_name`, `workflow_sent`, `status`, `asset_id`, `rule_asset_id`, `title_group_id`, `workflow_web_url`, `_deprecated_publication_status`, `published_at`, `_prev_put_up`, `_prev_take_down`, `_pending_simulation`, `workflow_template_url`, `original_draft_scheduling_id`, `playlist_id`, `updating_playlist`, `workflow_job_url`, `workflow_status`, `conflict_types`, `metadata_updated_at`, `company_id`, `cached_title_episode_number`, `metadata_status`, `publication_status`, `publication_status_updated_at`, `metadata_status_updated_at`, `external_id`, `disabled_at`, `scheduling_type`, `overridden_rule_attributes`, `update_in_progress`, `metadata_error_digest`) VALUES/g' $DUMP_PATH/schedulings-insert-tb.sql

echo "Truncate schedulings table"
echo "truncate $TB_DATABASE.$SCHEDULINGS_TABLE" | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn

echo "Loading schedulings into CH"
cat $DUMP_PATH/schedulings-insert-tb.sql | ~/tinybird/bin/ch/ch-20.7.2.30/ClickHouse/build/programs/clickhouse-client -mn
echo "Schedulings loaded"

echo "Process finished!"