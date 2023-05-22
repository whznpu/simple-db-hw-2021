package simpledb.storage;

public class Table {
    private DbFile file;
    private String name;
    private String pkeyField;

    public Table(DbFile file,String name,String pkeyField){
        this.file=file;
        this.name=name;
        this.pkeyField=pkeyField;
    }

    public DbFile getDbFile(){
        return this.file;
    }
    public String getName(){
        return  this.name;
    }

    public String getPkeyField(){
        return this.pkeyField;
    }

}
