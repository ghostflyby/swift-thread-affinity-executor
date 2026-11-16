//
//  SendingEnforcementTests.swift
//  BlockingQueueTests
//
//  Compile-time enforcement of the ownership handoff: a non-`Sendable`
//  copyable element must not be usable after enqueueing. This is the
//  property that makes the relaxed element constraint sound, so it is
//  pinned by an actual compiler run rather than assumed.
//

import Foundation
import Testing

private enum CompileCheckError: Error {
  case toolchainUnavailable
  case productsNotFound
}

private var compileCheckSupported: Bool {
  #if os(macOS)
    return true
  #else
    return false
  #endif
}

#if os(macOS)
  private func swiftcPath() throws -> String {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/xcrun")
    process.arguments = ["--find", "swiftc"]
    let stdout = Pipe()
    process.standardOutput = stdout
    process.standardError = Pipe()
    try process.run()
    process.waitUntilExit()
    let data = stdout.fileHandleForReading.readDataToEndOfFile()
    let path =
      String(data: data, encoding: .utf8)?
      .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    guard process.terminationStatus == 0, !path.isEmpty else {
      throw CompileCheckError.toolchainUnavailable
    }
    return path
  }

  /// Walks up from this source file to the package root, then locates the
  /// build directory containing both the compiled `BlockingQueue` module and
  /// its dependencies (only the products directory qualifies — intermediate
  /// build directories hold scattered copies).
  private func productsDirectory() throws -> String {
    let testsDir = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    let packageRoot = testsDir.deletingLastPathComponent().deletingLastPathComponent()
    let buildRoot = packageRoot.appendingPathComponent(".build", isDirectory: true)

    let enumerator = FileManager.default.enumerator(atPath: buildRoot.path)
    while let candidate = enumerator?.nextObject() as? String {
      guard candidate.hasSuffix("BlockingQueue.swiftmodule") else { continue }
      let containing =
        buildRoot
        .appendingPathComponent((candidate as NSString).deletingLastPathComponent)
      if FileManager.default.fileExists(
        atPath: containing.appendingPathComponent("DequeModule.swiftmodule").path)
      {
        return containing.path
      }
    }
    throw CompileCheckError.productsNotFound
  }

  /// Full compile (`-c`) of the snippet against the built module. The
  /// caller-side `sending` check lives in the SIL sendability pass, which
  /// `-typecheck` skips — a plain typecheck reports the violation as clean.
  private func compile(
    _ source: String, products: String, swiftc: String
  ) throws -> (status: Int32, diagnostics: String) {
    let dir = FileManager.default.temporaryDirectory
      .appendingPathComponent("bq-compile-check-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: dir) }

    let file = dir.appendingPathComponent("check.swift")
    try source.write(to: file, atomically: true, encoding: .utf8)

    let process = Process()
    process.executableURL = URL(fileURLWithPath: swiftc)
    process.arguments = [
      "-c", "-swift-version", "6", "-strict-concurrency=complete",
      "-I", products, file.path, "-o", dir.appendingPathComponent("check.o").path,
    ]
    let diagnostics = Pipe()
    process.standardOutput = Pipe()
    process.standardError = diagnostics
    try process.run()
    process.waitUntilExit()
    let output =
      String(
        data: diagnostics.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8
      ) ?? ""
    return (process.terminationStatus, output)
  }
#endif

/// The `sending` overload selected for copyable elements must keep
/// well-behaved non-`Sendable` use compiling (control), and must reject
/// use-after-enqueue of a non-`Sendable` element (violation). Verified
/// against the built module with a real compiler run, so the guarantee
/// holds for ordinary cross-module clients of this library.
@Test(.enabled(if: compileCheckSupported))
func nonSendableUseAfterEnqueueIsRejectedAtCompileTime() throws {
  #if os(macOS)
    let control = """
      import BlockingQueue

      final class Payload {
        var v = 0
      }

      func behave(_ producer: SharedQueueProducer<Payload>) {
        let p = Payload()
        producer.enqueue(p)
      }
      """

    let violation = """
      import BlockingQueue

      final class Payload {
        var v = 0
      }

      func violate(_ producer: SharedQueueProducer<Payload>) {
        let p = Payload()
        producer.enqueue(p)
        p.v += 1
      }
      """

    let swiftc: String
    let products: String
    do {
      swiftc = try swiftcPath()
      products = try productsDirectory()
    } catch {
      Issue.record("compile-check harness unavailable: \(error)")
      return
    }

    let controlResult = try compile(control, products: products, swiftc: swiftc)
    #expect(
      controlResult.status == 0,
      "control snippet must compile — got:\n\(controlResult.diagnostics)"
    )

    let violationResult = try compile(violation, products: products, swiftc: swiftc)
    #expect(violationResult.status != 0, "use-after-enqueue must not compile")
    #expect(
      violationResult.diagnostics.contains("sending"),
      "expected the use-after-send diagnostic — got:\n\(violationResult.diagnostics)"
    )
  #endif
}
