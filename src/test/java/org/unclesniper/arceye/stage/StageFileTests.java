package org.unclesniper.arceye.stage;

import java.io.File;
import org.junit.Test;
import java.io.IOException;
import java.io.FileOutputStream;
import static org.assertj.core.api.Assertions.assertThat;

public class StageFileTests {

	public static StageFile makeStage(boolean truncate) throws IOException {
		File file = File.createTempFile("stage", null);
		file.deleteOnExit();
		return new StageFile(file, truncate);
	}

	@Test
	public void truncate() throws IOException {
		File file = File.createTempFile("stage", null);
		file.deleteOnExit();
		try(FileOutputStream stream = new FileOutputStream(file)) {
			stream.write(new byte[] {1, 2, 3});
		}
		assertThat(file.length()).isEqualTo(3l);
		new StageFile(file, false).close();
		assertThat(file.length()).isEqualTo(3l);
		new StageFile(file, true).close();
		assertThat(file.length()).isZero();
	}

}
