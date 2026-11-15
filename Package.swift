// swift-tools-version: 6.2
import PackageDescription

let package = Package(
  name: "ThreadAffinityExecutor",
  platforms: [
    .macOS(.v15),
    .iOS(.v18),
    .tvOS(.v18),
    .watchOS(.v11),
    .visionOS(.v2),
  ],
  products: [
    .library(
      name: "BlockingQueue",
      targets: ["BlockingQueue"]
    ),
    .library(
      name: "ThreadAffinityExecutor",
      targets: ["ThreadAffinityExecutor"]
    ),
  ],
  dependencies: [
    .package(
      url: "https://github.com/apple/swift-collections.git",
      from: "1.4.0"
    )
  ],
  targets: [
    .target(
      name: "BlockingQueue",
      dependencies: [
        .product(
          name: "DequeModule",
          package: "swift-collections"
        )
      ],
      path: "Sources/BlockingQueue"
    ),
    .target(
      name: "ThreadAffinityExecutor",
      dependencies: [
        "BlockingQueue"
      ],
      path: "Sources/ThreadAffinityExecutor"
    ),
    .testTarget(
      name: "BlockingQueueTests",
      dependencies: ["BlockingQueue"],
      path: "Tests/BlockingQueueTests"
    ),
    .testTarget(
      name: "ThreadAffinityExecutorTests",
      dependencies: ["ThreadAffinityExecutor"],
      path: "Tests/ThreadAffinityExecutorTests"
    ),
  ]
)
