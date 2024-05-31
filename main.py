from generate_scripts import TableMigration

if __name__ == "__main__":
    migrate = TableMigration("schema_config.yml", "credentials.yml")
    migrate.generate_create_table_script()
    print('\n Complete')
