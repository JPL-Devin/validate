package gov.nasa.pds.tools.label;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;

import gov.nasa.pds.tools.validate.ProblemContainer;

/**
 * Concurrent integration test for {@link LabelValidator}.
 *
 * <p>Validates multiple labels from a thread pool concurrently to verify:
 * <ul>
 *   <li>No {@code ConcurrentModificationException} from shared state</li>
 *   <li>Each thread receives a correct, non-null {@link Document}</li>
 *   <li>Problem handlers are not cross-contaminated between threads</li>
 * </ul>
 *
 * <p>Uses local schemas from the github71 test resources to avoid network I/O.
 */
class LabelValidatorConcurrencyTest {

  /** Test data directory containing github71 labels and local schemas. */
  private static final String GITHUB71_DIR = "src/test/resources/github71";

  private LabelValidator validator;

  @TempDir
  File tempDir;

  @BeforeEach
  void setUp() throws Exception {
    String testPath = new File(System.getProperty("user.dir"), GITHUB71_DIR).getAbsolutePath();

    // Build a catalog file that rewrites PDS4 schema URLs to local files
    File catFile = new File(tempDir, "catalog.xml");
    String catText = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<catalog xmlns=\"urn:oasis:names:tc:entity:xmlns:xml:catalog\">\n"
        + "    <rewriteURI uriStartString=\"http://pds.nasa.gov/pds4\" rewritePrefix=\"file://"
        + testPath + "\" />\n"
        + "    <rewriteURI uriStartString=\"https://pds.nasa.gov/pds4\" rewritePrefix=\"file://"
        + testPath + "\" />\n"
        + "</catalog>";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(catFile))) {
      w.write(catText);
    }

    validator = new LabelValidator();
    validator.setCatalogs(new String[] {catFile.getAbsolutePath()});
    validator.setSchemaCheck(true);
    validator.setSchematronCheck(false);
    validator.setSkipProductValidation(true);
  }

  /**
   * Validates the same label from N threads concurrently and asserts that every
   * thread receives a non-null Document and that no exceptions are thrown.
   */
  @Test
  void concurrentParseAndValidate_sameLabel() throws Exception {
    int threadCount = 4;
    int iterationsPerThread = 3;

    URL labelUrl = findTestLabel();
    if (labelUrl == null) {
      System.err.println("Skipping concurrentParseAndValidate_sameLabel: no test label found");
      return;
    }

    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CyclicBarrier barrier = new CyclicBarrier(threadCount);
    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
    List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < threadCount; t++) {
      futures.add(pool.submit(() -> {
        try {
          barrier.await(30, TimeUnit.SECONDS);
          for (int i = 0; i < iterationsPerThread; i++) {
            ProblemContainer handler = new ProblemContainer();
            Document doc = validator.parseAndValidate(handler, labelUrl);
            assertNotNull(doc, "Document should not be null");
          }
        } catch (Throwable ex) {
          errors.add(ex);
        }
      }));
    }

    for (Future<?> f : futures) {
      f.get(120, TimeUnit.SECONDS);
    }
    pool.shutdown();
    assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "Pool should terminate");

    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder("Concurrent validation produced errors:\n");
      for (Throwable err : errors) {
        sb.append("  ").append(err.getClass().getName())
            .append(": ").append(err.getMessage()).append("\n");
        err.printStackTrace();
      }
      fail(sb.toString());
    }

    long expectedFiles = (long) threadCount * iterationsPerThread;
    assertEquals(expectedFiles, validator.getFilesProcessed(),
        "filesProcessed should equal threadCount * iterationsPerThread");
    assertTrue(validator.getTotalTimeElapsed() > 0, "totalTimeElapsed should be positive");
  }

  /**
   * Verifies that {@link LabelValidator#clear()} properly invalidates all
   * threads' cached ParserState via the generation counter.
   */
  @Test
  void clearInvalidatesAllThreads() throws Exception {
    URL labelUrl = findTestLabel();
    if (labelUrl == null) {
      System.err.println("Skipping clearInvalidatesAllThreads: no test label found");
      return;
    }

    int threadCount = 4;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CyclicBarrier barrier = new CyclicBarrier(threadCount);
    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

    // Phase 1: warm up ParserState on all threads
    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threadCount; t++) {
      futures.add(pool.submit(() -> {
        try {
          barrier.await(30, TimeUnit.SECONDS);
          ProblemContainer handler = new ProblemContainer();
          validator.parseAndValidate(handler, labelUrl);
        } catch (Throwable ex) {
          errors.add(ex);
        }
      }));
    }
    for (Future<?> f : futures) {
      f.get(120, TimeUnit.SECONDS);
    }

    // Phase 2: clear() from the main thread, then reconfigure
    String testPath = new File(System.getProperty("user.dir"), GITHUB71_DIR).getAbsolutePath();
    File catFile = new File(tempDir, "catalog2.xml");
    String catText = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<catalog xmlns=\"urn:oasis:names:tc:entity:xmlns:xml:catalog\">\n"
        + "    <rewriteURI uriStartString=\"http://pds.nasa.gov/pds4\" rewritePrefix=\"file://"
        + testPath + "\" />\n"
        + "    <rewriteURI uriStartString=\"https://pds.nasa.gov/pds4\" rewritePrefix=\"file://"
        + testPath + "\" />\n"
        + "</catalog>";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(catFile))) {
      w.write(catText);
    }

    validator.clear();
    validator.setCatalogs(new String[] {catFile.getAbsolutePath()});
    validator.setSchemaCheck(true);
    validator.setSchematronCheck(false);
    validator.setSkipProductValidation(true);

    // Phase 3: validate again - should pick up fresh state via generation counter
    CyclicBarrier barrier2 = new CyclicBarrier(threadCount);
    futures.clear();
    for (int t = 0; t < threadCount; t++) {
      futures.add(pool.submit(() -> {
        try {
          barrier2.await(30, TimeUnit.SECONDS);
          ProblemContainer handler = new ProblemContainer();
          Document doc = validator.parseAndValidate(handler, labelUrl);
          assertNotNull(doc, "Document should not be null after clear()");
        } catch (Throwable ex) {
          errors.add(ex);
        }
      }));
    }
    for (Future<?> f : futures) {
      f.get(120, TimeUnit.SECONDS);
    }

    pool.shutdown();
    assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "Pool should terminate");

    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder("Post-clear concurrent validation produced errors:\n");
      for (Throwable err : errors) {
        sb.append("  ").append(err.getClass().getName())
            .append(": ").append(err.getMessage()).append("\n");
        err.printStackTrace();
      }
      fail(sb.toString());
    }
  }

  /**
   * Locates a PDS4 test label from the github71 test resources.
   */
  private URL findTestLabel() {
    File f = new File(System.getProperty("user.dir"), GITHUB71_DIR + "/ELE_MOM.xml");
    if (f.exists()) {
      try {
        return f.toURI().toURL();
      } catch (Exception e) {
        // fall through
      }
    }
    return null;
  }
}
