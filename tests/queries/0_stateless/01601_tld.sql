SELECT throwIf(firstSignificantSubdomain('com.ss') == 'com');
SELECT throwIf(firstSignificantSubdomain('com.ss') != 'ss');
