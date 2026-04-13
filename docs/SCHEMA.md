# SQLite Schema Summary

## technologies
Stores the primary normalized technology records.

## technology_prerequisites
Stores graph edges where `tech_id` depends on `prerequisite_tech_id`.

## technology_categories
Stores multi-valued categories per technology.

## unlockables
Stores other data objects that reference technologies, such as buildings or components.

## technology_unlocks
Join table from technologies to unlockables.

## localisation
Stores localisation key/value pairs for the selected language.

## warnings
Stores parser and linkage warnings found during scan.
