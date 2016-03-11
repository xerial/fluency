package org.komamitsu.fluency.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileBackup
{
    private static final Logger LOG = LoggerFactory.getLogger(FileBackup.class);
    private final File backupDir;
    private final Buffer buffer;
    private final Pattern pattern;

    public static class SavedBuffer
        implements Closeable
    {
        private final List<String> params;
        private final File savedFile;
        private FileChannel channel;

        public SavedBuffer(File savedFile, List<String> params)
        {
            this.savedFile = savedFile;
            this.params = params;
        }

        public void open(Callback callback)
        {
            try {
                channel = new RandomAccessFile(savedFile, "r").getChannel();
                callback.process(params, channel.map(FileChannel.MapMode.PRIVATE, 0, savedFile.length()));
                success();
            }
            catch (Exception e) {
                LOG.error("Failed to process file. Skipping the file: file=" + savedFile, e);
            }
            finally {
                try {
                    close();
                }
                catch (IOException e) {
                    LOG.warn("Failed to close file: file=" + savedFile, e);
                }
            }
        }

        private void success()
        {
            try {
                close();
            }
            catch (IOException e) {
                LOG.warn("Failed to close file: file=" + savedFile, e);
            }
            finally {
                if (!savedFile.delete()) {
                    LOG.warn("Failed to delete file: file=" + savedFile);
                }
            }
        }

        @Override
        public void close()
                throws IOException
        {
            if (channel != null) {
                channel.close();
                channel = null;
            }
        }

        public interface Callback
        {
            void process(List<String> params, ByteBuffer buffer);
        }
    }

    public FileBackup(File backupDir, Buffer buffer)
    {
        this.backupDir = backupDir;
        this.buffer = buffer;
        this.pattern = Pattern.compile(buffer.bufferFormatType() + "#([\\w#]+\\).buf");
    }

    public List<SavedBuffer> getSavedFiles()
    {
        File[] files = backupDir.listFiles();
        ArrayList<SavedBuffer> savedBuffers = new ArrayList<SavedBuffer>();
        for (File f : files) {
            Matcher matcher = pattern.matcher(f.getName());
            if (matcher.find()) {
                List<String> params = new ArrayList<String>(matcher.groupCount());
                for (int i = 0; i < matcher.groupCount(); i++) {
                    params.add(matcher.group(i + 1));
                }
                savedBuffers.add(new SavedBuffer(f, params));
            }
        }
        return savedBuffers;
    }
}
