package org.mapdb;

import ch.unibas.dmi.dbis.cottontail.storage.store.PersistentStore;

import java.io.IOException;

public class MappedFileStoreDataInput extends DataInput2 {

    private long pos = 0;
    private final PersistentStore store;

    public MappedFileStoreDataInput(PersistentStore store, long offset) {
        this.store = store;
        this.pos = offset;
    }

    @Override
    public int getPos() {
        return (int)this.pos;
    }

    @Override
    public void setPos(int pos) {
        this.pos = (long)pos;
    }


    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        pos+=len;
        this.store.getData(this.pos, b, off, len);
    }

    @Override
    public int skipBytes(final int n) throws IOException {
        pos +=n;
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return this.store.get(pos++) ==1;
    }

    @Override
    public byte readByte() throws IOException {
        return this.store.get(pos++);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return this.store.get(pos++)& 0xff;
    }

    @Override
    public short readShort() throws IOException {
        final short ret = this.store.getShort(pos);
        pos+=2;
        return ret;
    }
    @Override
    public char readChar() throws IOException {
        //$DELAY$
        return (char) (
                ((this.store.get(pos++) & 0xff) << 8) |
                        (this.store.get(pos++) & 0xff));
    }

    @Override
    public int readInt() throws IOException {
        final int ret = this.store.getInt(pos);
        pos+=4;
        return ret;
    }

    @Override
    public long readLong() throws IOException {
        final long ret = this.store.getLong(pos);
        pos+=8;
        return ret;
    }

    @Override
    public float readFloat() throws IOException {
        final float ret = this.store.getFloat(pos);
        pos+=4;
        return ret;
    }

    @Override
    public double readDouble() throws IOException {
        final double ret = this.store.getDouble(pos);
        pos+=8;
        return ret;
    }

    @Override
    public byte[] internalByteArray() {
        return null;
    }

    @Override
    public java.nio.ByteBuffer internalByteBuffer() {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public long unpackLong() throws IOException {
        long ret = 0;
        byte v;
        do{
            v = this.store.get(pos++);
            ret = (ret<<7 ) | (v & 0x7F);
        }while((v&0x80)==0);

        return ret;
    }

    @Override
    public int unpackInt() throws IOException {
        int ret = 0;
        byte v;
        do{
            v = this.store.get(pos++);
            ret = (ret<<7 ) | (v & 0x7F);
        }while((v&0x80)==0);

        return ret;
    }


    @Override
    public long[] unpackLongArrayDeltaCompression(final int size) throws IOException {
        long[] ret = new long[size];
        int pos2 = (int)pos;
        long prev=0;
        byte v;
        for(int i=0;i<size;i++){
            long r = 0;
            do {
                v = this.store.get(pos2++);
                r = (r << 7) | (v & 0x7F);
            }while((v&0x80)==0);
            prev+=r;
            ret[i]=prev;
        }
        pos = pos2;
        return ret;
    }

    @Override
    public void unpackLongArray(long[] array, int start, int end) {
        int pos2 = (int)pos;
        long ret;
        byte v;
        for(;start<end;start++) {
            ret = 0;
            do {
                //$DELAY$
                v = this.store.get(pos2++);
                ret = (ret << 7) | (v & 0x7F);
            }while((v&0x80)==0);
            array[start] = ret;
        }
        pos = pos2;

    }

    @Override
    public void unpackLongSkip(int count) {
        int pos2 = (int)pos;
        while(count>0){
            count -= (this.store.get(pos2++)&0x80)>>7;
        }
        pos = pos2;
    }


    @Override
    public void unpackIntArray(int[] array, int start, int end) {
        int pos2 = (int)pos;
        int ret;
        byte v;
        for(;start<end;start++) {
            ret = 0;
            do {
                v = this.store.get(pos2++);
                ret = (ret << 7) | (v & 0x7F);
            }while((v&0x80)==0);
            array[start] = ret;
        }
        pos = pos2;
    }
}
