
CREATE_NICHIJOU_TABLE_COMMANDS = {
	'CREATE_TABLE_ANIME': (
		'CREATE TABLE IF NOT EXISTS `anime` ('
		'	`nid`			INT UNSIGNED NOT NULL,'
		'	PRIMARY KEY ( `nid` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	),
	'CREATE_TABLE_ANIME_NAME': (
		'CREATE TABLE IF NOT EXISTS `anime_name` ('
		'	`nid`			INT UNSIGNED NOT NULL,'
		'	`name`			VARCHAR(200) NOT NULL,'
		'	PRIMARY KEY ( `nid`, `name` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	),
	'CREATE_TABLE_EPISODE_NAME': (
		'CREATE TABLE IF NOT EXISTS `episode_name` ('
		'	`nid`			INT UNSIGNED NOT NULL,'
		'	`type`			INT UNSIGNED NOT NULL,'
		'	`sort`			INT UNSIGNED NOT NULL,'
		'	`name`			VARCHAR(200) NOT NULL,'
		'	PRIMARY KEY ( `nid`, `type`, `sort`, `name` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	),
	'CREATE_TABLE_MATCH_FAIL': (
		'CREATE TABLE IF NOT EXISTS `match_fail` ('
		'	`id`			INT UNSIGNED NOT NULL,'
		'	`source`		VARCHAR(40) NOT NULL,'
		'	`dis`			LONGTEXT NOT NULL,'
		'	PRIMARY KEY ( `id`, `source` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	),
	'CREATE_TABLE_CONFLICT': (
		'CREATE TABLE IF NOT EXISTS `conflict` ('
		'	`nid`			INT UNSIGNED NOT NULL,'
		'	`field`			VARCHAR(40) NOT NULL,'
		'	PRIMARY KEY ( `nid`, `field` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	),
	'CREATE_TABLE_REVISE': (
		'CREATE TABLE IF NOT EXISTS `revise` ('
		'	`nid`			INT UNSIGNED NOT NULL,'
		'	`field`			VARCHAR(40) NOT NULL,'
		'	`target`		VARCHAR(40),'
		'	`value`			LONGTEXT,'
		'	PRIMARY KEY ( `nid`, `field`, `target` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	),
	'CREATE_TABLE_SITES': (
		'CREATE TABLE IF NOT EXISTS `sites` ('
		'	`name`			VARCHAR(40) NOT NULL,'
		'	`urlTemplate`	LONGTEXT NOT NULL,'
		'	`regions`		LONGTEXT NOT NULL,'
		'	PRIMARY KEY ( `name` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	),
	'CREATE_TABLE_SOURCES': (
		'CREATE TABLE IF NOT EXISTS `sources` ('
		'	`name`			VARCHAR(40) NOT NULL,'
		'	`url`			LONGTEXT NOT NULL,'
		'	PRIMARY KEY ( `name` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	),
	'CREATE_TABLE_LOG': (
		'CREATE TABLE IF NOT EXISTS `log` ('
		'	`time`		DATETIME NOT NULL,'
		'	`content`	LONGTEXT NOT NULL'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	)
}

CREATE_ANIME_TABLE_COMMANDS = {
	'CREATE_TABLE_ANIME': (
		'CREATE TABLE IF NOT EXISTS `anime` ('
		'	`id`			INT UNSIGNED NOT NULL,'
		'	`url`			LONGTEXT NOT NULL,'
		'	`name`			VARCHAR(200) NOT NULL,'
		'	`name_cn`		VARCHAR(200),'
		'	`desc`			LONGTEXT,'
		'	`eps_cnt`		INT UNSIGNED,'
		'	`date`			DATE,'
		'	`weekday`		INT	UNSIGNED,'
		'	`meta`			LONGTEXT,'
		'	`tags`			LONGTEXT,'
		'	`type`			VARCHAR(10),'
		'	`image`			LONGTEXT,'
		'	`rating`		DECIMAL(32,28) UNSIGNED,'
		'	`rank`			INT UNSIGNED,'
		'	`related`		LONGTEXT,'
		'	`sites`			LONGTEXT,'
		'	PRIMARY KEY ( `id` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	), 
	'CREATE_TABLE_EPISODE': (
		'CREATE TABLE IF NOT EXISTS `episode` ('
		'	`id`			INT UNSIGNED NOT NULL,'
		'	`type`			INT UNSIGNED NOT NULL,'
		'	`sort`			INT UNSIGNED NOT NULL,'
		'	`url`			LONGTEXT NOT NULL,'
		'	`name`			VARCHAR(200),'
		'	`name_cn`		VARCHAR(200),'
		'	`status`		INT UNSIGNED NOT NULL,'
		'	`duration`		INT UNSIGNED,'
		'	`date`			DATE,'
		'	`desc`			LONGTEXT,'
		'	`sites`			LONGTEXT,'
		'	PRIMARY KEY ( `id`, `type`, `sort` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	), 
	'CREATE_TABLE_ANIME_NAME': (
		'CREATE TABLE IF NOT EXISTS `anime_name` ('
		'	`id`			INT UNSIGNED NOT NULL,'
		'	`name`			VARCHAR(200) NOT NULL,'
		'	PRIMARY KEY ( `id`, `name` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	), 
	'CREATE_TABLE_EPISODE_NAME': (
		'CREATE TABLE IF NOT EXISTS `episode_name` ('
		'	`id`			INT UNSIGNED NOT NULL,'
		'	`type`			INT UNSIGNED NOT NULL,'
		'	`sort`			INT UNSIGNED NOT NULL,'
		'	`name`			VARCHAR(200) NOT NULL,'
		'	PRIMARY KEY ( `id`, `type`, `sort`, `name` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	), 
	'CREATE_TABLE_LOG': (
		'CREATE TABLE IF NOT EXISTS `log` ('
		'	`time`			DATETIME NOT NULL,'
		'	`content`		LONGTEXT NOT NULL'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	), 
	'CREATE_TABLE_REQUEST_FAILED': (
		'CREATE TABLE IF NOT EXISTS `request_failed` ('
		'	`url_md5`		VARCHAR(32) NOT NULL,'
		'	`url`			LONGTEXT NOT NULL,'
		'	`spider`		VARCHAR(20) NOT NULL,'
		'	`desc`			LONGTEXT,'
		'	`params`		LONGTEXT,'
		'	PRIMARY KEY ( `url_md5`, `spider` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	), 
	'CREATE_TABLE_CACHE': (
		'CREATE TABLE IF NOT EXISTS `cache` ('
		'	`url_md5`		VARCHAR(32) NOT NULL,'
		'	`url`			LONGTEXT NOT NULL,'
		'	`expire`		DATETIME NOT NULL,'
		'	`content`		LONGBLOB,'
		'	PRIMARY KEY ( `url_md5` )'
		') ENGINE=InnoDB CHARSET=utf8mb4'
	)
}
