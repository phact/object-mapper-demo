package com.phact;

/**
 * Created by sebastianestevez on 5/31/16.
 *
 */

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.base.Objects;

import static com.datastax.driver.mapping.Mapper.Option.saveNullFields;


public class ObjectMapperWriter {

    public static void main(String args[]){
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    //.addContactPoint("127.0.0.1")
                    .addContactPoint("54.183.166.39")
                    .build();
            Session session = cluster.connect();                                           // (2)

            MappingManager manager = new MappingManager(session);
            Mapper<SparseTable> m = manager.mapper(SparseTable.class);


            m.setDefaultSaveOptions(saveNullFields(false));

            BatchStatement batch = new BatchStatement();
            for (int i=0; i< 100000000; i++) {
                //String value = String.valueOf(Math.floor((Math.random()*1000000000)));
                String value = String.valueOf(i);
                System.out.println(value);
                SparseTable row1 = new SparseTable(value);
                batch.add(m.saveQuery(row1));
                System.out.println("add: " + i);
                if ( i%9 ==0 ){
                    session.execute(batch);
                    batch.clear();
                    System.out.println("execute: " +i);
                }
            }

            System.out.println("Done");
        }  catch(Exception e){
            System.out.println(e.toString());
        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }


    }



    /**
     CREATE TABLE testks.sparsetable (
     field1 text PRIMARY KEY,
     field0 text,
     field10 text,
     field100 text,
     field101 text,
     field102 text,
     field103 text,
     field104 text,
     field105 text,
     field106 text,
     field107 text,
     field108 text,
     field109 text,
     field11 text,
     field110 text,
     field111 text,
     field112 text,
     field113 text,
     field114 text,
     field115 text,
     field116 text,
     field117 text,
     field118 text,
     field119 text,
     field12 text,
     field120 text,
     field121 text,
     field122 text,
     field123 text,
     field124 text,
     field125 text,
     field126 text,
     field127 text,
     field128 text,
     field129 text,
     field13 text,
     field130 text,
     field131 text,
     field132 text,
     field133 text,
     field134 text,
     field135 text,
     field136 text,
     field137 text,
     field138 text,
     field139 text,
     field14 text,
     field140 text,
     field141 text,
     field142 text,
     field143 text,
     field144 text,
     field145 text,
     field146 text,
     field147 text,
     field148 text,
     field149 text,
     field15 text,
     field16 text,
     field17 text,
     field18 text,
     field19 text,
     field2 text,
     field20 text,
     field21 text,
     field22 text,
     field23 text,
     field24 text,
     field25 text,
     field26 text,
     field27 text,
     field28 text,
     field29 text,
     field3 text,
     field30 text,
     field31 text,
     field32 text,
     field33 text,
     field34 text,
     field35 text,
     field36 text,
     field37 text,
     field38 text,
     field39 text,
     field4 text,
     field40 text,
     field41 text,
     field42 text,
     field43 text,
     field44 text,
     field45 text,
     field46 text,
     field47 text,
     field48 text,
     field49 text,
     field5 text,
     field50 text,
     field51 text,
     field52 text,
     field53 text,
     field54 text,
     field55 text,
     field56 text,
     field57 text,
     field58 text,
     field59 text,
     field6 text,
     field60 text,
     field61 text,
     field62 text,
     field63 text,
     field64 text,
     field65 text,
     field66 text,
     field67 text,
     field68 text,
     field69 text,
     field7 text,
     field70 text,
     field71 text,
     field72 text,
     field73 text,
     field74 text,
     field75 text,
     field76 text,
     field77 text,
     field78 text,
     field79 text,
     field8 text,
     field80 text,
     field81 text,
     field82 text,
     field83 text,
     field84 text,
     field85 text,
     field86 text,
     field87 text,
     field88 text,
     field89 text,
     field9 text,
     field90 text,
     field91 text,
     field92 text,
     field93 text,
     field94 text,
     field95 text,
     field96 text,
     field97 text,
     field98 text,
     field99 text
     )
     */

    @Table(name = "sparsetable",keyspace = "testks",
            readConsistency = "ONE",
            writeConsistency = "ONE")
    public static class SparseTable{

        public String getField3() {
            return field3;
        }

        public void setField3(String field3) {
            this.field3 = field3;
        }

        public String getField4() {
            return field4;
        }

        public void setField4(String field4) {
            this.field4 = field4;
        }

        public String getField5() {
            return field5;
        }

        public void setField5(String field5) {
            this.field5 = field5;
        }

        public String getField1() {
            return field1;
        }

        public void setField1(String field1) {
            this.field1 = field1;
        }

        public String getField0() {
            return field0;
        }

        public void setField0(String field0) {
            this.field0 = field0;
        }

        public String getField2() {
            return field2;
        }

        public void setField2(String field2) {
            this.field2 = field2;
        }

        @PartitionKey
        @Column(name = "field1")
        private String field1;
        private String field0;
        private String field2;
        private String field3;
        private String field4;
        private String field5;
        private String field6;
        private String field7;
        private String field8;
        private String field9;
        private String field10;
        private String field11;
        private String field12;
        private String field13;
        private String field14;
        private String field15;
        private String field16;
        private String field17;
        private String field18;
        private String field19;
        private String field20;
        private String field21;
        private String field22;
        private String field23;
        private String field24;
        private String field25;

        public String getField6() {
            return field6;
        }

        public void setField6(String field6) {
            this.field6 = field6;
        }

        public String getField7() {
            return field7;
        }

        public void setField7(String field7) {
            this.field7 = field7;
        }

        public String getField8() {
            return field8;
        }

        public void setField8(String field8) {
            this.field8 = field8;
        }

        public String getField9() {
            return field9;
        }

        public void setField9(String field9) {
            this.field9 = field9;
        }

        public String getField10() {
            return field10;
        }

        public void setField10(String field10) {
            this.field10 = field10;
        }

        public String getField11() {
            return field11;
        }

        public void setField11(String field11) {
            this.field11 = field11;
        }

        public String getField12() {
            return field12;
        }

        public void setField12(String field12) {
            this.field12 = field12;
        }

        public String getField13() {
            return field13;
        }

        public void setField13(String field13) {
            this.field13 = field13;
        }

        public String getField14() {
            return field14;
        }

        public void setField14(String field14) {
            this.field14 = field14;
        }

        public String getField15() {
            return field15;
        }

        public void setField15(String field15) {
            this.field15 = field15;
        }

        public String getField16() {
            return field16;
        }

        public void setField16(String field16) {
            this.field16 = field16;
        }

        public String getField17() {
            return field17;
        }

        public void setField17(String field17) {
            this.field17 = field17;
        }

        public String getField18() {
            return field18;
        }

        public void setField18(String field18) {
            this.field18 = field18;
        }

        public String getField19() {
            return field19;
        }

        public void setField19(String field19) {
            this.field19 = field19;
        }

        public String getField20() {
            return field20;
        }

        public void setField20(String field20) {
            this.field20 = field20;
        }

        public String getField21() {
            return field21;
        }

        public void setField21(String field21) {
            this.field21 = field21;
        }

        public String getField22() {
            return field22;
        }

        public void setField22(String field22) {
            this.field22 = field22;
        }

        public String getField23() {
            return field23;
        }

        public void setField23(String field23) {
            this.field23 = field23;
        }

        public String getField24() {
            return field24;
        }

        public void setField24(String field24) {
            this.field24 = field24;
        }

        public String getField25() {
            return field25;
        }

        public void setField25(String field25) {
            this.field25 = field25;
        }

        public String getField26() {
            return field26;
        }

        public void setField26(String field26) {
            this.field26 = field26;
        }

        public String getField27() {
            return field27;
        }

        public void setField27(String field27) {
            this.field27 = field27;
        }

        public String getField28() {
            return field28;
        }

        public void setField28(String field28) {
            this.field28 = field28;
        }

        public String getField29() {
            return field29;
        }

        public void setField29(String field29) {
            this.field29 = field29;
        }

        public String getField30() {
            return field30;
        }

        public void setField30(String field30) {
            this.field30 = field30;
        }

        public String getField31() {
            return field31;
        }

        public void setField31(String field31) {
            this.field31 = field31;
        }

        public String getField32() {
            return field32;
        }

        public void setField32(String field32) {
            this.field32 = field32;
        }

        private String field26;
        private String field27;
        private String field28;
        private String field29;
        private String field30;
        private String field31;
        private String field32;

        public SparseTable() {
        }

        public SparseTable(String value) {
            this.field1 = value;
            if(Math.random()>.7) {
                this.field0 = value;
            }
            if(Math.random()>.7) {
                this.field2 = value;
            }
            if(Math.random()>.7) {
                this.field3 = value;
            }
            if(Math.random()>.7) {
                this.field4 = value;
            }
            if(Math.random()>.7) {
                this.field5 = value;
            }
            if(Math.random()>.7) {
                this.field6 = value;
            }
            if(Math.random()>.7) {
                this.field7 = value;
            }
            if(Math.random()>.7) {
                this.field8 = value;
            }
            if(Math.random()>.7) {
                this.field9 = value;
            }
            if(Math.random()>.7) {
                this.field10 = value;
            }
            if(Math.random()>.7) {
                this.field11 = value;
            }
            if(Math.random()>.7) {
                this.field12 = value;
            }
            if(Math.random()>.7) {
                this.field13 = value;
            }
            if(Math.random()>.7) {
                this.field14 = value;
            }
            if(Math.random()>.7) {
                this.field15 = value;
            }
            if(Math.random()>.7) {
                this.field16 = value;
            }
            if(Math.random()>.7) {
                this.field17 = value;
            }
            if(Math.random()>.7) {
                this.field18 = value;
            }
            if(Math.random()>.7) {
                this.field19 = value;
            }
            if(Math.random()>.7) {
                this.field20 = value;
            }
            if(Math.random()>.7) {
                this.field21 = value;
            }
            if(Math.random()>.7) {
                this.field22 = value;
            }
            if(Math.random()>.7) {
                this.field23 = value;
            }
            if(Math.random()>.7) {
                this.field24 = value;
            }
            if(Math.random()>.7) {
                this.field25 = value;
            }
            if(Math.random()>.7) {
                this.field26 = value;
            }
            if(Math.random()>.7) {
                this.field27 = value;
            }
            if(Math.random()>.7) {
                this.field28 = value;
            }
            if(Math.random()>.7) {
                this.field29 = value;
            }
            if(Math.random()>.7) {
                this.field30 = value;
            }
            if(Math.random()>.7) {
                this.field31 = value;
            }
            if(Math.random()>.7) {
                this.field32 = value;
            }
        }


        @Override
        public boolean equals(Object other) {
            if (other == null || other.getClass() != this.getClass())
                return false;

            SparseTable that = (SparseTable) other;
            return Objects.equal(field0, that.field0)
                    && Objects.equal(field1, that.field1)
                    && Objects.equal(field2, that.field2);
        }

    }
}
